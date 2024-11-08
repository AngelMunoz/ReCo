open System
open System.Net.WebSockets
open System.Net
open System.Net.Http
open System.Text.Json
open System.Text.Json.Nodes
open System.Text.Json.Serialization
open System.Threading

open FSharp.Control
open FSharp.Control.Reactive
open IcedTasks
open IcedTasks.Polyfill.Async
open System.IO.Pipelines


type Commit = {
  rev: string
  operation: string
  collection: string
  rkey: string
  record: JsonNode option
  cid: string
}

type Identity = {
  did: string
  handle: string
  seq: int64
  time: string
}

type Account = {
  active: bool
  did: string
  seq: int64
  time: string
}

type Event = {
  did: string
  time_us: int64
  commit: Commit option
  account: Account option
  identity: Identity option
}

let prepareWebSocket() =
  let handler = new SocketsHttpHandler()
  let ws = new ClientWebSocket()
  ws.Options.HttpVersion <- HttpVersion.Version20
  ws.Options.HttpVersionPolicy <- HttpVersionPolicy.RequestVersionOrHigher

  handler, ws

let jsonOptions =
  JsonSerializerOptions(
    UnmappedMemberHandling = JsonUnmappedMemberHandling.Skip,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    DictionaryKeyPolicy = JsonNamingPolicy.SnakeCaseLower,
    PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
  )

let deserializeEvent(buffer: ReadOnlySpan<byte>) =
  try
    let result = JsonSerializer.Deserialize<Event>(buffer, jsonOptions)

    Ok result
  with ex ->
    let json = Text.Encoding.UTF8.GetString(buffer)
    Error(ex, json)

type JetStream =

  static member observe(uri, ?cancellationToken) =
    { new IObservable<_> with
        member _.Subscribe(observer: IObserver<_>) =
          let token = defaultArg cancellationToken CancellationToken.None
          let handler, ws = prepareWebSocket()

          async {
            do!
              ws.ConnectAsync(Uri(uri), new HttpMessageInvoker(handler), token)

            let pipe = new Pipe()
            let buffer = Array.zeroCreate(4096).AsMemory()

            while ws.State = WebSocketState.Open do
              let! result = ws.ReceiveAsync(buffer, token)
              let allocated = pipe.Writer.GetSpan(result.Count)

              buffer.Slice(0, result.Count).Span.CopyTo(allocated)
              pipe.Writer.Advance(result.Count)

              if result.EndOfMessage then
                let! _ = pipe.Writer.FlushAsync(token)
                let! read = pipe.Reader.ReadAsync(token)

                let memory =
                  read.Buffer.Slice(read.Buffer.Start, read.Buffer.Length)

                let deserializationResult =
                  let payload = memory.FirstSpan
                  deserializeEvent(payload)

                observer.OnNext deserializationResult
                pipe.Reader.AdvanceTo(read.Buffer.End)

              if token.IsCancellationRequested then
                do!
                  ws.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Cancelled",
                    token
                  )

            match ws.State with
            | WebSocketState.Aborted
            | WebSocketState.CloseReceived ->
              // Notify that we finished abnormally
              observer.OnError(exn "The connection was closed")
            | WebSocketState.CloseSent
            | WebSocketState.Closed -> observer.OnCompleted()
            | _ ->
              // It would be very weird if we ended up here
              ()
          }
          |> Async.Start

          { new IDisposable with
              member _.Dispose() =
                handler.Dispose()
                ws.Dispose()
          }
    }


let cts = new CancellationTokenSource()

let events =
  JetStream.observe(
    "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    cts.Token
  )

events
|> Observable.sample(TimeSpan.FromMilliseconds(720))
|> Observable.add(fun values ->
  match values with
  | Ok value -> printfn $"%A{value}"
  | Error(ex, json) -> eprintfn $"%s{json}")

Console.CancelKeyPress.Add(fun _ -> cts.Cancel())

Console.ReadLine() |> ignore
