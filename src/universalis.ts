import {defer, delay, from, mergeMap, Observable, of, pluck, retry, Subject, switchMapTo, tap} from "rxjs";
import axios from "axios";
import axiosRetry from "axios-retry";

axiosRetry(axios, {retries: 3, retryDelay: c => Math.pow(c, 2) * 10000})

const queue$: Subject<{ req: Observable<any>, res$: Subject<any> }> = new Subject<{ req: Observable<any>; res$: Subject<any> }>();

queue$.pipe(
    mergeMap(({req, res$}) => {
        return of(null).pipe(
            delay(1000 / 24),
            switchMapTo(req),
            tap(result => {
                res$.next(result)
                res$.complete()
            })
        )
    }, 5)
).subscribe()

export function doUniversalisRequest<T = any>(url: string, errors$: Subject<{ source: string, message: string }>): Observable<T> {
    const res$ = new Subject<any>()
    queue$.next({
        req: defer(() => {
            return from(axios.get(url).catch(err => {
                errors$.next({source: `[Universalis] ${url}`, message: err.message})
                console.error(err.message)
                throw err
            })).pipe(
                retry({
                    count: 10,
                    delay: (error, retryCount) => of(retryCount * 20000),
                    resetOnSuccess: true
                }),
                pluck('data')
            )
        }),
        res$
    });
    return res$;
}
