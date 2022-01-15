import express from 'express';
import cors from 'cors';
import {createRedisClient} from "./common";

const app = express();
app.use(cors());

function getScore(item: any, selfSufficient = true): number {
    let baseScore = item.profit.c - item.complexity * 10;
    if (!selfSufficient) {
        baseScore = item.profit.c - item.cost;
    }
    return baseScore * item.v24;
}

(async () => {
    const redis = await createRedisClient();

    app.get('/status', async (req, res) => {
        const status = await redis.get('status');
        res.send(status);
    });

    app.get('/crafting', async (req, res) => {
        const server = req.query.server as string;
        const levels = (req.query.levels as string)?.split(',').map(l => +l);
        const minV24 = req.query.minVelocity || 10;
        const maxComplexity = req.query.maxComplexity || 99999;
        const selfSufficient = req.query.selfSufficient === 'true';
        const serverCache = await redis.get(`profit:${server}`);
        const serverUpdated = await redis.get(`profit:${server}:updated`);
        if (!serverCache) {
            return res.status(500).send({message: `Profit cache for ${server} is missing`});
        }
        const serverCacheArray: any[] = JSON.parse(serverCache);
        const matching = serverCacheArray.filter(row => {
            return row.crafting
                && row.profit.c < 99999999
                && row.complexity < maxComplexity
                && row.v24 > minV24
                && row.levelReqs.every((lvl: number, i: number) => levels[i] >= lvl);
        }).sort((a, b) => {
            if (!selfSufficient) {
                return getScore(b, false) - getScore(a, false);
            }
            return getScore(b) - getScore(a);
        }).slice(0, 20);
        return res.send({
            items: matching,
            updated: +(serverUpdated || 1642050201)
        });
    });

    app.get('/gathering', async (req, res) => {
        const server = req.query.server as string;
        const levels = (req.query.levels as string)?.split(',').map(l => +l);
        const minV24 = req.query.minVelocity || 10;
        const serverCache = await redis.get(`profit:${server}`);
        const serverUpdated = await redis.get(`profit:${server}:updated`);
        if (!serverCache) {
            return res.status(500).send({message: `Profit cache for ${server} is missing`});
        }
        const serverCacheArray: any[] = JSON.parse(serverCache);
        const matching = serverCacheArray.filter(row => {
            return row.gathering
                && row.profit.c < 99999999
                && row.complexity < 99999
                && row.v24 > minV24
                && (row.levelReqs.every((lvl: number, i: number) => levels[i] >= lvl));
        }).sort((a, b) => {
            return getScore(b) - getScore(a);
        }).slice(0, 20);
        return res.send({
            items: matching,
            updated: +(serverUpdated || 1642050201)
        });
    });

    app.listen(8080);
})();
