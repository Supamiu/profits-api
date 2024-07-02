import axios from "axios";
import {chunk, uniq} from "lodash";
import {
    BehaviorSubject,
    catchError,
    combineLatest,
    debounceTime,
    defer,
    first,
    from,
    Observable,
    of,
    repeat,
    ReplaySubject,
    scan,
    shareReplay,
    skip,
    Subject,
    throttleTime
} from "rxjs";
import {map, switchMap, tap} from "rxjs/operators";
import {createRedisClient, updateCache, updateItems} from "./common";
import {doUniversalisRequest} from "./universalis";
import {Item} from "./item";
import {intervalToDuration} from "date-fns";
import {exec} from "child_process";
import {GAME_SERVERS} from "./servers";

const items$ = new ReplaySubject<Record<number, Item>>();
const delayBetweenRuns = 3600000;
const updated$ = new Subject<void>();

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
    const extractsReq = await axios.get('https://raw.githubusercontent.com/ffxiv-teamcraft/ffxiv-teamcraft/staging/libs/data/src/lib/extracts/extracts.json');
    const recipesReq = await axios.get('https://raw.githubusercontent.com/ffxiv-teamcraft/ffxiv-teamcraft/staging/libs/data/src/lib/json/recipes.json');
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

const errors$ = new Subject<{ source: string, message: string }>();

errors$.pipe(
    throttleTime(60000),
).subscribe(({source, message}) => {
    axios.post(process.env.WEBHOOK, {
        content: null,
        embeds: [{
            title: message,
            description: `${source.slice(0, 256)}...`,
            color: 16711680
        }],
        username: 'Profits Helper Updater'
    }).catch(err => {
        console.log(`[DISCORD ERROR HOOK] ${err.message}`)
    });
})

console.log('Creating core data Observable');

const coreData$ = combineLatest([
    of(GAME_SERVERS),
    from(createRedisClient()),
    items$,
    doUniversalisRequest<number[]>('https://universalis.app/api/marketable', errors$)
]).pipe(
    shareReplay(1)
);

console.log('Creating full data scheduler');

axios.post(process.env.WEBHOOK, {
    embeds: [{
        title: 'Updater started',
        color: 5832650,
        description: `Updater process has been started, initializing now... expect an update starting in a couple of seconds.`,
    }],
    username: 'Profits Helper Updater'
});


coreData$.pipe(
    switchMap(([servers, redis, items, itemIds]) => {
        return defer(() => {
            const expectedDuration = intervalToDuration({start: 0, end: servers.length * 180000});
            axios.post(process.env.WEBHOOK, {
                embeds: [{
                    title: 'Full update starting',
                    color: 5814783,
                    description: `Starting full update for ${servers.length} servers, ${itemIds.length} items (${Math.ceil(itemIds.length / 100)} chunks, ${Math.ceil(itemIds.length / 100) * servers.length} requests), this is expected to take about **${expectedDuration.hours} hours and ${expectedDuration.minutes} minutes** and should be done on <t:${Math.floor(new Date(Date.now() + servers.length * 180000).getTime() / 1000)}>`,
                }],
                username: 'Profits Helper Updater'
            }).catch(err => console.log(err.message));
            return properConcat(servers.map(server => {
                    const chunks = chunk(itemIds, 100);
                    return of(chunks).pipe(
                        switchMap(() => {
                            const start = Date.now();
                            console.log(`Starting MB data aggregation for ${server}`);
                            return combineLatest(
                                chunks.map((ids) => {
                                    return updateItems(server, ids, errors$);
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
                                    errors$.next({source: `[Updater] Server ${server}`, message: err.message});
                                    console.log(err.message)
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
            )
        }).pipe(
            repeat({
                delay: delayBetweenRuns
            })
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
            value: `${Math.floor(totalTime / 1000 / result.length)}s`
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
                text: `Next update cycle in 1h`
            }
        }],
        username: 'Profits Helper Updater'
    };
    axios.post(process.env.WEBHOOK, report).catch(err => console.log(err.message));
    updated$.next(void 0);
});

// If no updates after an entire day, ping Miu in the monitoring channel !
updated$.pipe(debounceTime(86400000)).subscribe(() => {
    axios.post(process.env.WEBHOOK, {
        content: '<@194378871317987328>',
        embeds: [{
            title: 'No updates for more than a day',
            color: 16734296
        }],
        username: 'Profits Helper Updater'
    });
    exec('pm2 restart Updater');
});
