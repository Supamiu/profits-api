import axios from "axios";
import {chunk, uniq} from "lodash";
import {BehaviorSubject, combineLatest, from} from "rxjs";
import {map, switchMap, tap} from "rxjs/operators";
import {createRedisClient, updateCache, updateItems} from "./common";
import {closeUniversalisQueue, doUniversalisRequest} from "./universalis";
import {Item} from "./item";

let items: Record<number, Item> = {};

const itemsDone$ = new BehaviorSubject<boolean>(false);

combineLatest([
    from(axios.get('https://github.com/ffxiv-teamcraft/ffxiv-teamcraft/raw/staging/apps/client/src/assets/extracts/extracts.json')),
    from(axios.get('https://raw.githubusercontent.com/ffxiv-teamcraft/ffxiv-teamcraft/staging/apps/client/src/assets/data/recipes.json')),
]).subscribe(([res0, res1]) => {
    const extracts = res0.data;
    const recipes = res1.data;
    Object.values<any>(extracts)
        .filter(e => !e.sources.some((s: any) => s.type === -1))
        .forEach(extract => {
            const crafting = extract.sources.find((source: any) => source.type === 1)?.data || null;
            const gathering = extract.sources.find((source: any) => source.type === 7)?.data || null;
            const vendors = extract.sources.find((source: any) => source.type === 3)?.data || null;
            const trades = extract.sources.find((source: any) => source.type === 2)?.data || null;
            const reduction = extract.sources.find((source: any) => source.type === 4)?.data || null;
            const requirements = crafting ? recipes.find((r: any) => r.id.toString() === crafting[0].id.toString())?.ingredients : null
            items[extract.id] = {
                id: extract.id,
                crafting,
                gathering,
                vendors,
                trades,
                reduction,
                requirements
            }
        });
    itemsDone$.next(true);
    itemsDone$.complete();
});

(async () => {
    const redis = await createRedisClient();
    console.log('Starting');

    console.log('Fetching server list');
    from(axios.get('https://xivapi.com/servers')).pipe(
        switchMap((res) => {
            const servers = res.data as string[];
            return doUniversalisRequest('https://universalis.app/api/marketable').pipe(
                switchMap((itemIds: number[]) => {
                    console.log('Starting MB data aggregation');
                    return combineLatest(servers.map(server => {
                            const chunks = chunk(itemIds, 100);
                            return combineLatest(chunks.map((ids, index) => {
                                return updateItems(server, ids).pipe(
                                    tap(async () => {
                                        console.log(`${server}#${index + 1}/${chunks.length}`);
                                    })
                                );
                            }))
                        })
                    ).pipe(
                        map(res => res.flat())
                    )
                })
            );
        })
    ).subscribe(async res => {
        for (let row of res) {
            const itemIds = Object.keys(row.data);
            for (let id of itemIds) {
                await redis.set(`mb:${row.server}:${id}`, JSON.stringify(row.data[+id]));
            }
        }
        closeUniversalisQueue();
        updateCache(uniq(res.map(row => row.server)), items, redis);
    });
})()
