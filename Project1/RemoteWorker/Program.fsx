// Learn more about F# at http://docs.microsoft.com/dotnet/fsharp
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
                    let hashVal = i |> string |> Util.calculateSHA256

                    if checkInitialZeros (hashVal, k, 0) then
                        printf "%s\n" hashVal

                    sender <! Done("completed")
                    return! loop ()
             | _ -> ()
        }
    loop ()

printf "reaching here\n"

let RemoteBoss (ip_address: string) (mailbox: Actor<_>) =
    printf "%s\n" ip_address

    let serverUrl =
        $"akka.tcp://MinerServer@{ip_address}:{server_port}/user/ServerBossActor"

    printf "%s\n" serverUrl
    let serverRef = select serverUrl actorSystem

    let numberOfCores =
        System.Environment.ProcessorCount |> int64

    let numberOfChildActors = numberOfCores * 5000L

    let workerActorsPool =
        [ 1L .. numberOfChildActors ]
        |> List.map (fun id -> spawn actorSystem (sprintf "RemoteClient_%d" id) RemoteClient)

    let workerenum = [| for lp in workerActorsPool -> lp |]

    let workerSystem =
        actorSystem.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))

    let mutable splits = 0L
    let mutable completed = 0L
    let splitSize = numberOfChildActors * 2L
    printf "splitsize %d\n" splitSize

    let rec loop () =
        actor {
            let! (message: obj) = mailbox.Receive()
            let boss = mailbox.Sender()

            match message with
            | :? string as s ->
                match s with
                | "shutdown" -> 
                    printf "received shutdown signal\n"
                    mailbox.Context.System.Terminate() |> ignore
                | _ -> ()
            | :? Tuple<int64, int64, int> as t ->
                let (startInd, delta, k): Tuple<int64, int64, int> = downcast message
                printf "received message with startInd %d delta %d and k %d\n" startInd delta k
                splits <- delta / splitSize
                printf "splits %d\n" splits
                let mutable chunkStart = startInd
                let mutable chunkEnd = startInd + splitSize - 1L
                for i in 1L .. splits do
                    workerSystem <! Input(chunkStart, chunkEnd, k)
                    chunkStart <- chunkEnd + 1L
                    chunkEnd <- chunkEnd + splitSize
            | :? JobDetails as jd ->
                match jd with
                    | Join ("join") -> serverRef <! "Joining"
                    | Done (complete) ->
                        completed <- completed + 1L
                        //printf "completed %d\n" completed
                        if completed = splits then
                            printf "sending done signal\n"
                            serverRef <! "DoneRemote"
                            //mailbox.Context.System.Terminate() |> ignore
            return! loop ()
        }
    loop ()

let serverIpAddress =
    System.Environment.GetCommandLineArgs().[1]

let proc = Process.GetCurrentProcess()
let cpu_time_stamp = proc.TotalProcessorTime
let timer = new Stopwatch()
timer.Start()

try
    let remoteBossRef =
        spawn actorSystem "remoteBoss" (RemoteBoss serverIpAddress)
    remoteBossRef <! Join("join")
    actorSystem.WhenTerminated.Wait()
finally
    let cpu_time =
        (proc.TotalProcessorTime - cpu_time_stamp)
            .TotalMilliseconds

    printfn "CPU time = %dms" (int64 cpu_time)
    printfn "Absolute time = %dms" timer.ElapsedMilliseconds
