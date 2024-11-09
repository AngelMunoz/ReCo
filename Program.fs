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
open FsToolkit.ErrorHandling
open System.Collections.Generic

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

module JetStream =

  let startListening
    (
      ws: ClientWebSocket,
      jsonDeserializer: (ReadOnlyMemory<byte> -> 'T) option,
      token
    ) =
    let pipe = Pipe()

    let inline deserialize memory =
      match jsonDeserializer with
      | Some deserialize -> deserialize(memory)
      | None ->
        JsonSerializer.Deserialize<'T>(
          memory.Span,
          JsonSerializerOptions(
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Skip,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            DictionaryKeyPolicy = JsonNamingPolicy.SnakeCaseLower,
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
          )
        )

    taskSeq {
      while ws.State = WebSocketState.Open do
        let buffer = pipe.Writer.GetMemory(1024)
        let! result = ws.ReceiveAsync(buffer, token)
        pipe.Writer.Advance(result.Count)

        if result.EndOfMessage then
          let! _ = pipe.Writer.FlushAsync(token)
          let! read = pipe.Reader.ReadAsync(token)

          try
            let result = deserialize(read.Buffer.First)

            Ok result
          with ex ->
            let json = Text.Encoding.UTF8.GetString(read.Buffer.FirstSpan)
            Error(ex, json)

          pipe.Reader.AdvanceTo(read.Buffer.End)
    }

  let prepareWebSocket() =
    let handler = new SocketsHttpHandler()
    let ws = new ClientWebSocket()
    ws.Options.HttpVersion <- HttpVersion.Version20
    ws.Options.HttpVersionPolicy <- HttpVersionPolicy.RequestVersionOrHigher

    handler, ws


type JetStream =

  static member toAsyncSeq(uri, ?jsonDeserializer, ?cancellationToken) = taskSeq {
    let token = defaultArg cancellationToken CancellationToken.None
    let handler, ws = JetStream.prepareWebSocket()

    try
      do! ws.ConnectAsync(Uri(uri), new HttpMessageInvoker(handler), token)

      yield! JetStream.startListening(ws, jsonDeserializer, token)
    finally
      match ws.State with
      | WebSocketState.Aborted
      | WebSocketState.CloseReceived ->
        // Notify that we finished abnormally
        ws.Dispose()
        handler.Dispose()
        failwith "The connection was closed"
      | _ ->
        ws.Dispose()
        handler.Dispose()
  }


  static member toObservable(uri, ?jsonDeserializer, ?cancellationToken) =
    { new IObservable<_> with
        member _.Subscribe(observer: IObserver<_>) =
          let token = defaultArg cancellationToken CancellationToken.None
          let cts = CancellationTokenSource.CreateLinkedTokenSource(token)

          let work = async {
            try

              let values =
                JetStream.toAsyncSeq(
                  uri,
                  ?jsonDeserializer = jsonDeserializer,
                  cancellationToken = cts.Token
                )

              for value in values do
                observer.OnNext(value)
            with ex ->
              observer.OnError(ex)
          }

          Async.StartImmediate(work, cts.Token)

          { new IDisposable with
              member _.Dispose() = cts.Cancel()
          }
    }

let cts = new CancellationTokenSource()

let events =
  JetStream.toObservable<Event>(
    "wss://jetstream2.us-east.bsky.network/subscribe",
    cancellationToken = cts.Token
  )

let work =
  JetStream.toAsyncSeq(
    "wss://jetstream2.us-east.bsky.network/subscribe",
    cancellationToken = cts.Token
  )
  |> TaskSeq.iter(fun value ->
    match value with
    | Ok value -> printfn $"%s{value.did} - %A{value.commit}"
    | Error(ex, json) -> eprintfn $"%s{json}")
  |> Async.AwaitTask

Async.StartImmediate(work, cts.Token)

Console.CancelKeyPress.Add(fun _ -> cts.Cancel())

Console.ReadLine() |> ignore
cts.Cancel()
