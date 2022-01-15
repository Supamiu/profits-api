import axios from "axios";
import {chunk, uniq} from "lodash";
import {combineLatest, from, of, ReplaySubject, shareReplay, timer} from "rxjs";
import {exhaustMap, map, pluck, switchMap} from "rxjs/operators";
import {createRedisClient, updateCache, updateItems} from "./common";
import {doUniversalisRequest} from "./universalis";
import {Item} from "./item";

const items$ = new ReplaySubject<Record<number, Item>>();

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

const timers = {};

console.log('Creating core data Observable');
const coreData$ = combineLatest([
    from(axios.get('https://xivapi.com/servers')).pipe(pluck('data')),
    from(createRedisClient()),
    items$,
    doUniversalisRequest<number[]>('https://universalis.app/api/marketable')
]).pipe(
    shareReplay(1)
);

console.log('Creating full data scheduler');
coreData$.pipe(
    switchMap(([servers, redis, items, itemIds]) => {
        // Update 8 times a day per server, start after 30s to avoid colliding with the other scheduler
        return timer(30000, Math.floor(86400000 / 8 / servers.length)).pipe(
            exhaustMap(i => {
                const server = servers[i % (servers.length - 1)];
                timers[`FULL:${server}`] = Date.now();
                const chunks = chunk(itemIds, 100);
                console.log(`Starting MB data aggregation for ${server}`);
                return combineLatest(chunks.map((ids, index) => {
                    return updateItems(server, ids);
                })).pipe(
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
                    map(() => server)
                );
            })
        )
    })
).subscribe((server) => {
    const timeToUpdate = Date.now() - timers[`FULL:${server}`];
    sendToDiscord(`Full update for ${server} done in ${timeToUpdate.toLocaleString()}ms.`);
    delete timers[`FULL:${server}`];
});

function sendToDiscord(message: string): void {
    axios.post(process.env.WEBHOOK, {content: message});
}
