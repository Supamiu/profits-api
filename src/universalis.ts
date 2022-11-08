import {
    catchError,
    defer,
    delay,
    EMPTY,
    from,
    mergeMap,
    Observable,
    of,
    pluck,
    retry,
    Subject,
    switchMapTo,
    tap,
    timer
} from "rxjs";
import axios, {AxiosError} from "axios";

const queue$: Subject<{ req: Observable<any>, res$: Subject<any> }> = new Subject<{ req: Observable<any>; res$: Subject<any> }>();

queue$.pipe(
    mergeMap(({req, res$}) => {
        return of(null).pipe(
            delay(1000 / 15),
            switchMapTo(req),
            tap(result => {
                res$.next(result)
                res$.complete()
            })
        )
    }, 10)
).subscribe()

export function doUniversalisRequest<T = any>(url: string, errors$: Subject<{ source: string, message: string }>): Observable<T> {
    const res$ = new Subject<any>()
    queue$.next({
        req: defer(() => {
            return from(axios.get(url).catch((err: AxiosError) => {
                console.error(`[${err.response?.status}] ${err.message}\n${url}`);
                errors$.next({
                    source: `[Universalis] ${url}`,
                    message: `[${err.response?.status}] ${err.message}`
                })
                throw err
            }))
        }).pipe(
            retry({
                count: 10,
                delay: (error, retryCount) => timer(retryCount * 5000),
                resetOnSuccess: true
            }),
            catchError(() => EMPTY),
            pluck('data')
        ),
        res$
    });
    return res$;
}
