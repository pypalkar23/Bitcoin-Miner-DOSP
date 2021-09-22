#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#load "Util.fs"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open Akka.Remote
open System.Diagnostics
open Util


let server_port = 5000
let rwl = System.Threading.ReaderWriterLockSlim()
let configuration =
    ConfigurationFactory.ParseString(
        @"akka {
            log-config-on-start : on
            stdout-loglevel : DEBUG
            loglevel : ERROR
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote.helios.tcp {
                transport-protocol = tcp
                port = 5000
                hostname = 0.0.0.0
            }
        }"
    )


//Skeleton of Msg
type JobDetails =
    | Criteria of (int)
    | Input of (int64 * int64 * int)
    | Done of (string)
    | Found of (string)

let actorSystem =
    ActorSystem.Create("MinerServer", configuration)

let assignJobsToWorkers (childActorSystem: IActorRef, startInd:int64, splits:int64, splitSize:int64, numOfZeros:int) =
    let jobSize = 2000000000L
    printf "starting for startInd %d endInd %d ....\n" startInd (startInd+jobSize)
    let mutable chunkStart = startInd
    let mutable chunkEnd = startInd + splitSize - 1L
    for i in 1L .. splits do
        childActorSystem <! Input(chunkStart, chunkEnd, numOfZeros)
        chunkStart <- chunkEnd + 1L
        chunkEnd <- chunkEnd + splitSize


let PrinterActor (mailbox: Actor<_>)=
    let mutable coins = 0L
    let rec loop() = actor {
        let! (message:string) = mailbox.Receive()
        coins <- coins + 1L
        printfn "%d %s" coins message
        return! loop()
    }
    loop()

let ServerWorker (mailbox: Actor<_>) =
    let rec loop () =
        actor {
            let! (message: JobDetails) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with
            | Input (startInd, endInd, k) ->
                //printf "%d %d %d\n" k startInd endInd
                for i in startInd .. endInd do
                    //let hashVal = (startInd|>double) + (endInd|>double)* (Random().NextDouble()) |> string |> Util.calculateSHA256
                    let hashVal = i |> string |> Util.calculateSHA256

                    if checkInitialZeros (hashVal, k, 0) then
                        printf "%s\n" hashVal

                    sender <! Done("completed")
                    return! loop ()
            | _ -> ()
        }

    loop ()

let printerRef = 
        spawn actorSystem "PrinterActor" PrinterActor


let ServerBoss (mailbox: Actor<_>) =
    let numberOfCores =
        System.Environment.ProcessorCount |> int64

    let numberOfChildActors = numberOfCores * 250L

    let workerActorsPool =
        [ 1L .. numberOfChildActors ]
        |> List.map (fun id -> spawn actorSystem (sprintf "LocalClient_%d" id) ServerWorker)

    let workerenum = [| for lp in workerActorsPool -> lp |]

    let workerSystem =
        actorSystem.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))

    let mutable startInd = 1L
    let jobSize = 2000000000L
    let maxInd = 10000000000000L
    let mutable remoteMachinesConnected = 0
    let mutable completed = 0L
    let mutable currentEnd = jobSize
    let splitSize = numberOfChildActors * 2L
    let mutable numOfZeros = 0
    let splits = jobSize / splitSize
    printf "splits %d\n" splits
    printf "splitsize %d\n" splitSize

    let rec loop () =
        actor {
            let! (message: obj) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with
            | :? string as msg ->
                match msg with
                | "DoneRemote" ->
                    printf "currentEnd %d maxInd %d" currentEnd maxInd
                    if (currentEnd >= maxInd) then
                        sender <! "shutdown" 
                        remoteMachinesConnected <- remoteMachinesConnected - 1
                        if( completed = splits && remoteMachinesConnected = 0) then
                            mailbox.Context.System.Terminate()|> ignore    
                    elif (currentEnd < maxInd) then
                        startInd <- startInd+ jobSize
                        currentEnd <- currentEnd + jobSize
                        sender <! (currentEnd+1L, jobSize , numOfZeros)
                | "Joining" ->
                    printf "Remote Machine Connected\n"
                    remoteMachinesConnected <- remoteMachinesConnected + 1
                    sender <! (currentEnd + 1L, jobSize, numOfZeros)
                    currentEnd <- currentEnd + jobSize
            | :? JobDetails as jd ->
                match jd with
                | Criteria (k) ->
                    numOfZeros <- k
                    assignJobsToWorkers(workerSystem, startInd, splits, splitSize, numOfZeros)

                | Done (complete) -> 
                    if(completed < splits) then 
                        completed <- completed + 1L
                        if(completed = splits && currentEnd < maxInd) then
                            completed <- 0L
                            startInd <- startInd+ jobSize
                            currentEnd <- currentEnd + jobSize
                            assignJobsToWorkers(workerSystem, startInd, splits, splitSize, numOfZeros)
                    if (completed = splits && remoteMachinesConnected = 0 && currentEnd >= maxInd) then
                            mailbox.Context.System.Terminate() |> ignore                       
            | _ -> ()

            return! loop ()
        }
    loop ()

let serverBossRef =
    spawn actorSystem "ServerBossActor" ServerBoss

let proc = Process.GetCurrentProcess()
let cpu_time_stamp = proc.TotalProcessorTime
let timer = new Stopwatch()
timer.Start()


try
    serverBossRef <! Criteria(4)
    actorSystem.WhenTerminated.Wait()
finally
    let cpu_time = (proc.TotalProcessorTime - cpu_time_stamp).TotalMilliseconds
    printfn "CPU time = %dms" (int64 cpu_time)
    printfn "Absolute time = %dms" timer.ElapsedMilliseconds
