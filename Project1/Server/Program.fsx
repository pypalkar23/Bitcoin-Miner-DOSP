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

let UFID = "mandarpalkar"
let jobSize = 1000000L
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
    | Input of (int64 * int64 * int64)
    | Done of (string)
    | Join of (string)

let actorSystem =
    ActorSystem.Create("MinerServer", configuration)

let ServerWorker (mailbox: Actor<_>) =
    let rec loop () =
        actor {
            let! (message: JobDetails) = mailbox.Receive()
            let sender = mailbox.Sender()
            match message with
            | Input (startInd, endInd, k) ->
                printf "%d %d %d\n" k startInd endInd
                for i in startInd .. endInd do
                    ()
                sender <! Done("completed")
                return! loop ()
        }
    loop ()

printf "reaching here\n"

let RemoteBoss (mailbox: Actor<_>) =
    let numberOfCores =
        System.Environment.ProcessorCount |> int64

    let numberOfChildActors = numberOfCores * 250L

    let workerActorsPool =
        [ 1L .. numberOfChildActors ]
        |> List.map (fun id -> spawn actorSystem (sprintf "LocalClient_%d" id) ServerWorker)

    let workerenum = [| for lp in workerActorsPool -> lp |]

    let workerSystem =
        actorSystem.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))

    let mutable splits = 0L
    let mutable completed = 0L
    let splitSize = numberOfChildActors * 2L
    printf "splitsize %d\n" splitSize

    let rec loop () =
        actor {
            let! (message: JobDetails) = mailbox.Receive()
            let boss = mailbox.Sender()

            match message with
            | Input (startInd, delta, k) ->
                splits <- delta / splitSize
                printf "splits %d\n" splits
                let mutable chunkStart = startInd
                let mutable chunkEnd = startInd + splitSize - 1L
                for i in 1L .. splits do
                    workerSystem <! Input(chunkStart, chunkEnd, i)
                    chunkStart <- chunkEnd + 1L
                    chunkEnd <- chunkEnd + splitSize
            | Join (ip_address) ->
                let serverUrl =
                    $"akka.tcp://MinerServer@{ip_address}:{server_port}/user/ServerBossActor"
                let serverRef = actorSystem.ActorSelection(serverUrl)
                serverRef <! Join(ip_address)
            | Done (complete) ->
                completed <- completed + 1L
                //printf "completed %d\n" completed
                if completed = splits then
                    mailbox.Context.System.Terminate() |> ignore

            return! loop ()
        }
    loop ()

let remoteBossRef =
    spawn actorSystem "remoteBoss" RemoteBoss

   

let proc = Process.GetCurrentProcess()
let cpu_time_stamp = proc.TotalProcessorTime
let timer = new Stopwatch()
timer.Start()
try
    remoteBossRef <! Input(1L, 100000L, 4L)
    actorSystem.WhenTerminated.Wait()
finally
    let cpu_time = (proc.TotalProcessorTime-cpu_time_stamp).TotalMilliseconds
    printfn "CPU time = %dms" (int64 cpu_time)
    printfn "Absolute time = %dms" timer.ElapsedMilliseconds
