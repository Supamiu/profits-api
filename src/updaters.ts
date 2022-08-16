import axios from "axios";
import {chunk, uniq} from "lodash";
import {
    BehaviorSubject,
    catchError,
    combineLatest,
    defer,
    first,
    from,
    Observable,
    of,
    repeat,
    ReplaySubject,
    retry,
    scan,
    shareReplay,
    skip
} from "rxjs";
import {map, switchMap, tap} from "rxjs/operators";
import {createRedisClient, updateCache, updateItems} from "./common";
import {doUniversalisRequest} from "./universalis";
import {Item} from "./item";
import {intervalToDuration} from "date-fns";

const items$ = new ReplaySubject<Record<number, Item>>();
const delayBetweenRuns = 3600000;

function properConcat<T>(sources: Observable<T>[]): Observable<T[]> {
    const index$ = new BehaviorSubject<number>(0);
    return index$.pipe(
        switchMap(i => sources[i]),
        scan((acc, res) => [...acc, res], []),
        tap(() => {
            if (index$.value < sources.length - 1) {
                index$.next(index$.value + 1)
            } else {
                index$.complete();
            }
        }),
        skip(sources.length - 1),
        first()
    )
}

(async () => {
    console.log('Preparing items');
    const items = {};
    const extractsReq = await axios.get('https://github.com/ffxiv-teamcraft/ffxiv-teamcraft/raw/staging/apps/client/src/assets/extracts/extracts.json');
    const recipesReq = await axios.get('https://raw.githubusercontent.com/ffxiv-teamcraft/ffxiv-teamcraft/staging/apps/client/src/assets/data/recipes.json');
    const extracts = extractsReq.data;
    const recipes = recipesReq.data;
    Object.values<any>(extracts)
        .filter(e => !e.sources.some((s: any) => s.type === -1))
        .forEach(extract => {
            const crafting = extract.sources.find((source: any) => source.type === 1)?.data || null;
            const gathering = extract.sources.find((source: any) => source.type === 7)?.data || null;
            const vendors = extract.sources.find((source: any) => source.type === 3)?.data || null;
            const trades = extract.sources.find((source: any) => source.type === 2)?.data || null;
            const reduction = extract.sources.find((source: any) => source.type === 4)?.data || null;
            const requirements = crafting ? recipes.find((r: any) => r.id.toString() === crafting[0].id.toString())?.ingredients : null;
            items[extract.id] = {
                id: extract.id,
                crafting,
                gathering,
                vendors,
                trades,
                reduction,
                requirements
            };
        });
    items$.next(items);
})();

console.log('Creating core data Observable');
const coreData$ = combineLatest([
    from(axios.get('https://xivapi.com/servers')).pipe(map(res => res.data as string[])),
    from(createRedisClient()),
    items$,
    doUniversalisRequest<number[]>('https://universalis.app/api/marketable')
]).pipe(
    shareReplay(1)
);

console.log('Creating full data scheduler');

coreData$.pipe(
    switchMap(([servers, redis, items, itemIds]) => {
        return defer(() => properConcat(servers.map(server => {
                const chunks = chunk(itemIds, 100);
                return of(chunks).pipe(
                    switchMap(() => {
                        const start = Date.now();
                        console.log(`Starting MB data aggregation for ${server}`);
                        return combineLatest(
                            chunks.map((ids) => {
                                return updateItems(server, ids);
                            })
                        ).pipe(
                            switchMap(res => {
                                if (res.length === 0) {
                                    return of([]);
                                }
                                return combineLatest(res.map(row => {
                                    const itemIds = Object.keys(row.data);
                                    if (itemIds.length === 0) {
                                        return of([]);
                                    }
                                    return combineLatest(itemIds.map(id => {
                                        return from(redis.set(`mb:${row.server}:${id}`, JSON.stringify(row.data[+id])));
                                    }));
                                })).pipe(
                                    switchMap(() => {
                                        return from(updateCache(uniq(res.map(row => row.server)), items, redis));
                                    })
                                );
                            }),
                            switchMap(() => {
                                return from(redis.set(`profit:${server}:updated`, Date.now()))
                            }),
                            map(() => {
                                console.log(`${server} ok, ${Math.floor((Date.now() - start) / 1000)}s`);
                                return {
                                    server,
                                    success: true,
                                    time: Date.now() - start
                                }
                            }),
                            catchError((err) => {
                                console.log(err)
                                return of({
                                    server,
                                    success: false,
                                    time: Date.now() - start
                                })
                            }),
                        )
                    })
                );
            })
        )).pipe(
            repeat({
                delay: delayBetweenRuns
            }),
            retry()
        )
    })
).subscribe((result) => {
    const success = result.every(row => row.success);
    const failedServers = result.filter(row => !row.success).map(row => row.server);
    const totalTime = result.reduce((acc, r) => acc + r.time, 0);
    const duration = intervalToDuration({start: 0, end: totalTime});
    const fields = [
        {
            name: "Avg per server",
            value: `${totalTime / 1000 / result.length}s`
        },
        {
            name: "Total time for this run",
            value: `${duration.hours}h ${duration.minutes}min ${duration.seconds}s`
        }
    ];
    if (!success) {
        fields.push({
            name: 'Failed servers',
            value: failedServers.reduce((acc, server) => `${acc}\n - ${server}`)
        })
    }
    const report = {
        content: null,
        embeds: [{
            title: 'Full update status report',
            color: success ? 4169782 : 16734296,
            fields,
            footer: {
                text: `Next update cycle: ${new Date(Date.now() + delayBetweenRuns).toString()}`
            }
        }],
        username: 'Profits Helper Updater'
    };
    console.log(JSON.stringify(report));
    axios.post(process.env.WEBHOOK, report).catch(err => console.log(err.message));
});
