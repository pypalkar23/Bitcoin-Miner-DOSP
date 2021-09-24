module Util

open System.Text
open System.Security.Cryptography
let prefixKey = "mandar.palkar"


let calculateSHA256 (x:string) =
    x |> Encoding.ASCII.GetBytes |> (new SHA256Managed()).ComputeHash |> Seq.map( fun b -> b.ToString("x2")) |> String.concat ""


let rec checkInitialZeros (text:string,k:int,curr:int) =
        if(curr=k)then true
        elif not (text.[curr].Equals('0')) then false
        else checkInitialZeros (text,k,curr+1)

                
    

