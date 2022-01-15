import axios from "axios";
import {uniq} from "lodash";
import {BehaviorSubject, combineLatest, from, of} from "rxjs";
import {filter, switchMap, tap} from "rxjs/operators";
import {createRedisClient, updateCache, updateServerData} from "./common";
import {closeUniversalisQueue} from "./universalis";
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


combineLatest([
    itemsDone$.pipe(filter(done => done)),
    createRedisClient()
]).pipe(
    switchMap(([, redis]) => {
        return from(axios.get('https://xivapi.com/servers')).pipe(
            switchMap(res => {
                const servers: string[] = res.data;
                return combineLatest(servers.map(server => {
                    return updateServerData(server).pipe(
                        tap(() => console.log('UPDATED SERVER DATA', server))
                    );
                }));
            }),
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
            })
        )
    })
).subscribe(() => {
    closeUniversalisQueue();
    console.log('ALL DONE');
    process.exit(0);
});
