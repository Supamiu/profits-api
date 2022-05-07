import {
    catchError,
    from,
    interval,
    mapTo,
    mergeMap,
    Observable,
    of,
    ReplaySubject,
    retry,
    Subject,
    Subscription,
    timer
} from "rxjs";
import {filter, map, switchMap, tap} from "rxjs/operators";
import axios from "axios";
import axiosRetry from "axios-retry";


const UNIVERSALIS_REQ_PER_SECOND = 14;
const UNIVERSALIS_BURST = 14;

axiosRetry(axios, {retries: 3, retryDelay: c => Math.pow(c, 2) * 1000})

const queue: { url: string, res$: Subject<any> }[] = [];

let stop$ = new Subject();

let queueSub: Subscription | null = initQueue();

const requestsLog = {};

const ratesStatus = {
    avg: 0,
    burst: 0
};

// Start the queue consumer
setInterval(() => {
    const currentSeconds = new Date().getUTCSeconds();
    delete requestsLog[currentSeconds - 11];
    const requestsTotal = new Array(10)
        .fill(null)
        .map((_, i) => {
            return requestsLog[currentSeconds - i] || 15;
        })
        .reduce((acc, v) => acc + v);
    ratesStatus.avg = requestsTotal / 10;
    ratesStatus.burst = requestsLog[currentSeconds - 1] || 0;
}, 200);

function initQueue(): Subscription {
    return interval(1000 / UNIVERSALIS_REQ_PER_SECOND).pipe(
        filter(() => queue.length > 0),
        mergeMap(() => {
            if (queue.length === 0) {
                return of(null);
            }
            const {url, res$} = queue.shift();
            let delay$ = timer(0).pipe(mapTo(0));
            if (ratesStatus.burst > UNIVERSALIS_BURST || ratesStatus.avg > UNIVERSALIS_REQ_PER_SECOND - 2) {
                delay$ = timer(1000).pipe(mapTo(1000));
            } else {
                requestsLog[new Date().getUTCSeconds()] = (requestsLog[new Date().getUTCSeconds()] || 0);
                requestsLog[new Date().getUTCSeconds()] += 1;
            }
            return delay$.pipe(
                switchMap((delayed) => {
                    if (delayed > 0) {
                        requestsLog[new Date().getUTCSeconds()] = (requestsLog[new Date().getUTCSeconds()] || 0);
                        requestsLog[new Date().getUTCSeconds()] += 1;
                    }
                    return from(axios.get(url))
                }),
                map(res => {
                    return {
                        res,
                        res$
                    };
                }),
                retry({
                    count: 3,
                    delay: (error, retryCount) => of(Math.pow(retryCount, 2) * 1000)
                }),
                catchError(e => {
                    console.error(url, e.message);
                    queue.push({url, res$});
                    return of(null)
                })
            );
        }, 8),
    ).subscribe((entry) => {
        if (entry) {
            const {res$, res} = entry;
            res$.next(res.data);
            res$.complete();
        }
    });
}

export function doUniversalisRequest<T = any>(url: string): Observable<T> {
    const timeout = setTimeout(() => {
        queue.push({
            url, res$
        })
    }, 30000);
    const res$ = new ReplaySubject<T>();
    queue.push({
        url,
        res$
    });
    return res$.pipe(
        tap(() => {
            clearTimeout(timeout);
        })
    );
}

export function closeUniversalisQueue(): void {
    if (queueSub) {
        stop$.next(void 0);
        queueSub.unsubscribe();
    }
}
