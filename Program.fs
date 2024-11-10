open System
open System.Net.WebSockets
open System.Net
open System.Net.Http
open System.Text.Json
open System.Text.Json.Serialization
open System.Threading

open FSharp.Control
open FSharp.Control.Reactive
open IcedTasks
open IcedTasks.Polyfill.Async
open System.IO.Pipelines
open FsToolkit.ErrorHandling
open System.Collections.ObjectModel

type Commit = {
  rev: string
  operation: string
  collection: string
  rkey: string
  record: JsonDocument option
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
    (uri: Uri, jsonDeserializer: (ReadOnlyMemory<byte> -> 'T) option, token)
    =
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

      use handler = new SocketsHttpHandler()
      use ws = new ClientWebSocket()
      ws.Options.HttpVersion <- HttpVersion.Version20
      ws.Options.HttpVersionPolicy <- HttpVersionPolicy.RequestVersionOrHigher

      do! ws.ConnectAsync(uri, new HttpMessageInvoker(handler), token)

      while ws.State = WebSocketState.Open do
        if token.IsCancellationRequested then
          do!
            ws.CloseAsync(
              WebSocketCloseStatus.NormalClosure,
              "Cancelled",
              token
            )
        else

        let buffer = pipe.Writer.GetMemory(512)
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

      match ws.State with
      | WebSocketState.Aborted ->
        // Notify that we finished abnormally
        failwith "The connection was closed"
      | _ -> ()
    }

type JetStream =

  static member toAsyncSeq(uri, ?jsonDeserializer, ?cancellationToken) =
    let token = defaultArg cancellationToken CancellationToken.None

    JetStream.startListening(Uri(uri), jsonDeserializer, token)


  static member toObservable(uri, ?jsonDeserializer, ?cancellationToken) =
    { new IObservable<_> with
        member _.Subscribe(observer: IObserver<_>) =
          let token = defaultArg cancellationToken CancellationToken.None
          let cts = CancellationTokenSource.CreateLinkedTokenSource(token)

          let work = async {
            try
              do!
                JetStream.toAsyncSeq(
                  uri,
                  ?jsonDeserializer = jsonDeserializer,
                  cancellationToken = cts.Token
                )
                |> TaskSeq.iter observer.OnNext

              observer.OnCompleted()
            with ex ->
              observer.OnError(ex)
          }

          Async.StartImmediate(work, cts.Token)

          { new IDisposable with
              member _.Dispose() =
                cts.Cancel()
                cts.Dispose()
          }
    }


let cts = new CancellationTokenSource()

let events =
  JetStream.toObservable<Event>(
    "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    cancellationToken = cts.Token
  )

let buffered = events |> Observable.sample(TimeSpan.FromSeconds(1))


let obs = ObservableCollection<Event>()

buffered
|> Observable.add(fun e ->
  match e with
  | Ok e ->
    if obs.Count > 10 then
      obs.RemoveAt(0)
      obs.Add(e)
    else
      obs.Add(e)
  | Error(ex, json) -> printfn "Error: %s" json)


obs.CollectionChanged
|> Event.add(fun e ->
  printfn "Collection changed %A" e.Action

  for i in obs do
    printfn "%A -> %A" i (i.commit.Value.record.Value.RootElement.GetRawText()))


Console.CancelKeyPress.Add(fun _ -> cts.Cancel())

Console.ReadLine() |> ignore
cts.Cancel()
