// Learn more about F# at http://docs.microsoft.com/dotnet/fsharp
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#load "Util.fs"

open System
open System.Diagnostics
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open Util


let serverPort = 5000

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
                port = 5555
                hostname = 0.0.0.0
            }
        }"
    )


//Skeleton of Msg
type JobDetails =
    | Input of (int64 * int64 * int)
    | Done of (string)
    | Join of (string)
    | Found of (string)


let actorSystem =
    ActorSystem.Create("MinerRemote", configuration)

let RemoteClient (mailbox: Actor<_>) =
    let rec loop () =
        actor {
            let! (message: JobDetails) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with
            | Input (startInd, endInd, k) ->
                //printf "%d %d %d\n" k startInd endInd
                for i in startInd .. endInd do
                    let prefixedString = i |> string |> fun x-> prefixKey+x
                    let hashVal = prefixedString |> Util.calculateSHA256

                    if checkInitialZeros (hashVal, k, 0) then
                        sender <! Found(prefixedString+" "+hashVal)

                sender <! Done("completed")
                return! loop ()
            | _ -> ()
        }

    loop ()


let RemoteBoss (ipAddress: string) (mailbox: Actor<_>) =
    printf "connecting to server -> %s:%d\n" ipAddress serverPort

    let serverUrl =
        $"akka.tcp://MinerServer@{ipAddress}:{serverPort}/user/ServerBossActor"

    let printerUrl =
        $"akka.tcp://MinerServer@{ipAddress}:{serverPort}/user/PrinterActor"

    let serverRef = select serverUrl actorSystem
    let printerRef = select printerUrl actorSystem

    let numberOfCores = Environment.ProcessorCount |> int64

    let numberOfChildActors = numberOfCores * 250L

    let workerActorsPool =
        [ 1L .. numberOfChildActors ]
        |> List.map (fun id -> spawn actorSystem (sprintf "RemoteClient_%d" id) RemoteClient)

    let workerenum = [| for lp in workerActorsPool -> lp |]

    let workerSystem =
        actorSystem.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))

    let mutable splits = 0L
    let mutable splitsInProcess = 0L
    let mutable tempStart = 0L
    let mutable tempEnd = 0L
    let chunkSize = numberOfChildActors * 2L
        
    let rec loop () =
        actor {
            let! (message: obj) = mailbox.Receive()
     
            match message with
            | :? string as s ->
                match s with
                | "shutdown" ->
                    printf "Received order to shutdown .. no more jobs to process\n"
                    mailbox.Context.System.Terminate() |> ignore
                | _ -> ()
            | :? Tuple<int64, int64, int> as t ->
                let (startInd, endInd, k): Tuple<int64, int64, int> = downcast message
                printf "Received Message To Process Block %d-%d and k: %d\n" startInd endInd k
                splits <- (endInd - startInd + 1L) / chunkSize
                tempStart <- startInd
                tempEnd <- endInd
                let mutable chunkStart = startInd
                let mutable chunkEnd = startInd + chunkSize - 1L
                splitsInProcess <- splits
                for i in 1L .. splits do
                    workerSystem <! Input(chunkStart, chunkEnd, k)
                    chunkStart <- chunkEnd + 1L
                    chunkEnd <- chunkEnd + chunkSize
            | :? JobDetails as jd ->
                match jd with
                | Join ("join") -> serverRef <! "Joining"
                | Done (complete) ->
                    splitsInProcess <- splitsInProcess - 1L
                    if (splitsInProcess = 0L) then 
                        printf "Finished Processing For Block %d-%d\n" tempStart tempEnd
                        serverRef <! "DoneRemote"
                | Found (coinValue) -> printerRef <! coinValue+ " r" //suffixing remote machines coins with r 
                | _ -> ()
            | _ -> ()

            return! loop ()
        }

    loop ()

let serverIpAddress = Environment.GetCommandLineArgs()|> fun x -> if (x.Length=2) then x.[1] else "0.0.0.0" 
let proc = Process.GetCurrentProcess()
let cpuTimeStamp = proc.TotalProcessorTime
let timer = new Stopwatch()
timer.Start()

try
    let remoteBossRef =
        spawn actorSystem "remoteBoss" (RemoteBoss serverIpAddress)

    remoteBossRef <! Join("join")
    actorSystem.WhenTerminated.Wait()
finally
    let cpu_time =
        (proc.TotalProcessorTime - cpuTimeStamp)
            .TotalMilliseconds

    printfn "CPU time = %dms" (int64 cpu_time)
    printfn "Absolute time = %dms" timer.ElapsedMilliseconds
