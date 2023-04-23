import axios from "axios";
import {BehaviorSubject, combineLatest, from} from "rxjs";
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
