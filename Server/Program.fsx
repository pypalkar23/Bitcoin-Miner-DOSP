#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#load "Util.fs"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Diagnostics
open Util

let prefixKey = "mandar.palkar"
let server_port = 5000

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

let actorSystem =
    ActorSystem.Create("MinerServer", configuration)

let assignJobsToWorkers
    (
        childActorSystem: IActorRef,
        startInd: int64,
        splits: int64,
        splitSize: int64,
        numOfZeros: int
    ) =
    let mutable chunkStart = startInd
    let mutable chunkEnd = startInd + splitSize - 1L

    for i in 1L .. splits do
        childActorSystem
        <! Input(chunkStart, chunkEnd, numOfZeros)

        chunkStart <- chunkEnd + 1L
        chunkEnd <- chunkEnd + splitSize


let PrinterActor (mailbox: Actor<_>) =
    let mutable coins = 0L

    let rec loop () =
        actor {
            let! (message: string) = mailbox.Receive()
            coins <- coins + 1L
            printf "%d %s\n" coins message
            return! loop ()
        }
    loop ()

let printerRef =
        spawn actorSystem "PrinterActor" PrinterActor    

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
                    let prefixedString = i |> string |> fun x-> prefixKey+x
                    let hashVal = prefixedString |> Util.calculateSHA256

                    if checkInitialZeros (hashVal, k, 0) then
                        printerRef <! (prefixedString+" "+hashVal)   
                sender <! Done("completed")
            | _ -> ()
            return! loop ()
        }
    loop ()



let ServerSubordinateActor (mailbox: Actor<_>) =
    let numberOfCores =
        System.Environment.ProcessorCount |> int64

    let numberOfChildActors = numberOfCores * 250L
    let splitSize = numberOfChildActors * 2L
    let workerActorsPool =
        [ 1L .. numberOfChildActors ]
        |> List.map (fun id -> spawn actorSystem (sprintf "LocalClient_%d" id) ServerWorker)

    let workerenum = [| for lp in workerActorsPool -> lp |]

    let workerSystem =
        actorSystem.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))
   
    let mutable inProgressSplits = 0L
    let mutable tempStart = 0L
    let mutable tempEnd = 0L
    let rec loop () = 
        actor { 
            let! (message:JobDetails) = mailbox.Receive()
            let sender = mailbox.Sender()
            match message with
                | Input(startInd, endInd, k) ->
                    //printf "calculating for block %d-%d...\n" startInd endInd
                    tempStart <- startInd
                    tempEnd <- endInd
                    inProgressSplits <- (endInd-startInd+1L)/splitSize
                    assignJobsToWorkers (workerSystem,startInd,inProgressSplits,splitSize,k)
                | Done (text) -> 
                    inProgressSplits <- inProgressSplits - 1L
                    if (inProgressSplits = 0L) then
                        //printf "done for block %d-%d...\n" tempStart tempEnd
                        mailbox.Context.Parent <! Done("done")
                | _ ->()
            return! loop() 
        }

    loop ()


let ServerBoss (mailbox: Actor<_>) =
    let serverSubActorRef = spawn mailbox.Context "ServerSubActor" ServerSubordinateActor
    let mutable startInd = 1L
    let jobSize = 2000000L
    let multiplier = 100000000L
    let mutable maxInd = 0L
    //let mutable blocksInProgress = maxInd/jobSize       
    let mutable numOfZeros = 0
    let mutable localBlocksInProgress = 0L
    let mutable remoteBlocksInProgress = 0L
    
    let rec loop () =
        actor {
            let! (message: obj) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with
            | :? string as msg ->
                match msg with
                | "DoneRemote" ->
                       remoteBlocksInProgress <- remoteBlocksInProgress - 1L
                       if (startInd>=maxInd && remoteBlocksInProgress = 0L) then
                         if (localBlocksInProgress = 0L) then
                            sender <! "shutdown"
                         //remoteMachinesConnected <- remoteMachinesConnected - 1
                            //printf "In remote calculation block startInd %d\n" startInd 
                            remoteBlocksInProgress <- remoteBlocksInProgress + 1L
                            mailbox.Context.System.Terminate()|>ignore
                       else
                         sender <! (startInd, startInd+jobSize-1L, numOfZeros)
                         remoteBlocksInProgress <- remoteBlocksInProgress + 1L  
                | "Joining" ->
                        if (startInd>=maxInd) then 
                          sender <! "shutdown"
                        else
                          sender <! (startInd, startInd+jobSize-1L, numOfZeros)
                          remoteBlocksInProgress <- remoteBlocksInProgress + 1L
            | :? JobDetails as jd ->
                match jd with
                | Criteria (k) -> 
                    numOfZeros <- k
                    maxInd <- multiplier * (k|>int64)
                    serverSubActorRef <! Input(startInd,startInd+jobSize-1L,numOfZeros)
                    localBlocksInProgress <-  localBlocksInProgress + 1L
                | Done (complete) ->
                    localBlocksInProgress <-  localBlocksInProgress - 1L
                    if (startInd>=maxInd) then
                        if (localBlocksInProgress = 0L && remoteBlocksInProgress = 0L) then
                            //printf "connected: %d maxInd %d" blocksInProgress maxInd
                            mailbox.Context.System.Terminate()|>ignore
                    else
                        serverSubActorRef <! Input(startInd,startInd+jobSize-1L,numOfZeros)
                        localBlocksInProgress <-  localBlocksInProgress + 1L
            | _ -> ()            
            startInd <- startInd + jobSize                
            return! loop ()
        }
    loop ()

let serverBossRef =
    spawn actorSystem "ServerBossActor" ServerBoss

let zeroCountStr = System.Environment.GetCommandLineArgs() |> fun x -> if (x.Length = 2) then x.[1] else "4"
let zeroCount = zeroCountStr |> int
let proc = Process.GetCurrentProcess()
let cpuTimeStamp = proc.TotalProcessorTime
let timer = new Stopwatch()
timer.Start()

try
    serverBossRef <! Criteria(zeroCount)
    actorSystem.WhenTerminated.Wait()
finally
    let cpuTime =
        (proc.TotalProcessorTime - cpuTimeStamp)
            .TotalMilliseconds

    printfn "CPU time = %dms" (cpuTime |> int64)
    printfn "Absolute time = %dms" timer.ElapsedMilliseconds
