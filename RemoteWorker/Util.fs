module Util

open System.Text
open System.Security.Cryptography
open System
let prefixKey = "mandar.palkar"


let calculateSHA256 (x:string) =
    x |> Encoding.ASCII.GetBytes |> (new SHA256Managed()).ComputeHash |> Seq.map( fun b -> b.ToString("x2")) |> String.concat ""


let rec checkInitialZeros (text:string,k:int,curr:int) =
        if(curr=k)then true
        elif not (text.[curr].Equals('0')) then false
        else checkInitialZeros (text,k,curr+1)

                
               
            
//printfn "%s" (calculateSHA256("10000"));  
//printfn "%s" ((checkInitialZeros("000005",4,0))|>string) 

