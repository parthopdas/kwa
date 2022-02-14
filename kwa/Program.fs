open System
open System.Net
open HtmlAgilityPack
open Kwa

[<Struct>]
type Article =
    { Title: string
      Url: Uri
      Date: DateTime
      Tags: string list }

type CsvRecord = 
    | Article of Article
    | ArticleError of string

    static member toCsvLine = function
        | Article a -> 
            let tags = 
                if (List.isEmpty a.Tags) then
                    [ String.Empty ]
                else 
                    a.Tags
            tags |> List.map (sprintf "%O,%O,%O,%O" a.Date (a.Title.Trim().Replace(",", "|")) a.Url)
        | ArticleError s -> [ s ]

let getUrlForPage =
    sprintf "http://blog.kaggle.com/category/winners-interviews/page/%d/"
    >> Uri

let getPage (uri: Uri) = 
    async {
        use client = new WebClient()
        try
            printfn " [%O] Started ..." uri
            let! html = uri |> client.DownloadStringTaskAsync |> Async.AwaitTask
            printfn " [%O] ... finished" uri
            return Ok html
        with
        | ex -> 
            printfn " [%O] ... exception" ex.Message
            let err = sprintf "[%s] %A" uri.Host ex.Message
            return Error err
    }

let extractArticleFromNode (node: HtmlNode): Result<Article, string> = 
    try 
        let entryTitle = node.SelectSingleNode(".//*[@class='entry-title']")
        let tags = 
            node.SelectNodes(".//footer/a")
            |> Option.ofNull
            |> Option.getOrElse (HtmlNodeCollection(node))
            |> Seq.map (fun a -> a.InnerText)
            |> Seq.toList
        let url = entryTitle.SelectSingleNode("a").Attributes.["href"].Value |> Uri
        let date = node.SelectSingleNode(".//*[@class='entry-date']").Attributes.["datetime"].Value |> DateTime.Parse

        { Title = entryTitle.InnerText
          Url = url
          Date = date
          Tags = tags } |> Ok
    with e -> Error e.Message

let extractArticlesFromPage (page: string): Result<Article, string> list =
    let doc = HtmlDocument()
    doc.LoadHtml(page)
    doc.DocumentNode.SelectNodes("//article")
    |> Seq.map extractArticleFromNode
    |> Seq.toList

[<EntryPoint>]
let main argv =
    [1..19]
    |> List.map (getUrlForPage >> getPage)
    |> List.sequenceAsyncA
    |> Async.map List.sequenceResultA
    |> Async.map (Result.map (List.map extractArticlesFromPage))
    |> Async.RunSynchronously
    |> Result.map (List.collect id)
    |> Result.fold (List.map <| Result.fold Article ArticleError) (ArticleError >> List.singleton)
    |> List.collect CsvRecord.toCsvLine
    |> List.iter (printf "%s\r\n")

    0

(*
let main2 argv =
    let x0: Async<Result<List<string>, string>> = 
        [1..19]
        |> List.map (getUrlForPage >> getPage)  // ( (int -> Uri) o (Uri -> Async<Result<string, string>>) )    -> List<Async<Result<string, string>>> 
        |> List.sequenceAsyncA                  // List<Async<Result<string, string>>>                          -> Async<List<Result<string, string>>>
        |> Async.map List.sequenceResultA       // Async<List<Result<string, string>>>                          -> Async<Result<List<string>, string>>>

    let x01 = //: Async<Result<List<List<Result<Article, string>>>, string>> = 
        x0
        |> Async.map (Result.map (List.map extractArticlesFromPage))    // Async<Result<List<string>, string>>> -> Async<Result<List<List<Result<Article, string>>>, string>>>
    
    let x1 =
        x01
        |> Async.RunSynchronously               // Async<Result<List<Article>, string>>>                        -> Result<List<Article>, string>>

    let x2 = 
        x1 
        |> Result.map (List.collect id)         // Result<List<Article>, string>>                               -> 
    
    x2
        |> Result.fold (List.map <| Result.fold Article ArticleError) (ArticleError >> List.singleton)
        |> List.collect CsvRecord.toCsvLine
        |> List.iter (printf "%s\r\n")

    0
*)