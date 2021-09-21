module Util

open System.Text
open System.Security.Cryptography
open System
let prefixKey = "mandar.palkar"


let calculateSHA256 (x:string) =
    x |> Encoding.ASCII.GetBytes |> (new SHA256Managed()).ComputeHash |> Seq.map( fun b -> b.ToString("x2")) |> String.concat ""
        


