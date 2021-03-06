(**
 * Copyright (c) 2014, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the "hack" directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 *
 *)

open Core

(*****************************************************************************)
(* Module building workers
 * A worker is a subprocess executing an arbitrary function
 * You should first create a fixed amount of workers and then use those
 * because the amount of workers is limited and to make the load-balancing
 * of tasks better (cf multiWorker.ml)
 *)
(*****************************************************************************)

(* The maximum amount of workers *)
let max_workers = 1000


(*****************************************************************************)
(* The job executed by the worker. *)
(*****************************************************************************)

type request =
  | Init: (Gc.control * SharedMem.handle) -> request
  | Request: (('a -> unit) -> unit) -> request

(*****************************************************************************)
(* Everything we need to know about a worker. *)
(*****************************************************************************)

type dummy

type t = {
  id: int;
  mutable job: job;
  heap_handle: SharedMem.handle;
  preforked: (dummy, request) Daemon.handle option;
  mutable killed: bool;
}
and job = Job : 'a slave -> job | NoJob : job

(*****************************************************************************)
(* The handle is what we get back when we start a job. It's a "future"
 * (sometimes called a "promise"). The scheduler uses the handle to retrieve
 * the result of the job when the task is done (cf multiWorker.ml).
 *)
(*****************************************************************************)
and 'a handle = 'a delayed ref
and 'a delayed =
  | Processing of 'a slave
  | Cached of 'a
  | Failed of exn
and 'a slave = {
  result: unit -> 'a;
  slave_pid: int;
  infd: Unix.file_descr;
  worker: t;
}

(*****************************************************************************)
(* Our polling primitive on workers
 * Given a list workers, returns the ones that a ready for more work.
 *)
(*****************************************************************************)

type 'a selected = {
  readys: 'a handle list;
  waiters: 'a handle list;
}

let get_processing ds =
  List.fold_left
    ~f:(fun ps d ->
      match !d with
      | Cached c -> ps
      | Failed exn -> ps
      | Processing p -> p::ps)
    ~init:[]
    ds

let select ds =
  let processing = get_processing ds in
  let fds = List.map ~f:(fun {infd; _} -> infd) processing in
  let ready_fds, _, _ =
    (* TODO when |processing| <> |ds| then timeout = 0. *)
    match fds with
    | [] -> [], [], []
    | fds -> Unix.select fds [] [] ~-.1. in
  List.fold_right
    ~f:(fun d { readys ; waiters } ->
      match !d with
      | Cached _ | Failed _ ->
          { readys = d :: readys ; waiters }
      | Processing s when List.mem ready_fds s.infd ->
          { readys = d :: readys ; waiters }
      | Processing _ ->
          { readys ; waiters = d :: waiters})
    ~init:{ readys = [] ; waiters = [] }
    ds

let slave_main ic oc =
  let send_result res =
    Marshal.to_channel oc res [];
    flush oc in
  try
    match Daemon.from_channel ic with
    | Request do_process ->
        Unix.handle_unix_error do_process send_result;
        exit 0
    | _ -> assert false
  with
  | End_of_file ->
      exit 1
  | e ->
      let e_str = Printexc.to_string e in
      Printf.printf "Exception: %s\n" e_str;
      EventLogger.worker_exception e_str;
      print_endline "Potential backtrace:";
      Printexc.print_backtrace stdout;
      exit 2

let win32_slave_main (ic, oc) =
  let oc = Daemon.cast_out oc in
  slave_main ic oc

let linux_slave_main (ic, oc) =
  let () =
    match Daemon.from_channel ic with
    | Init (gc_control, heap_handle) ->
        SharedMem.connect heap_handle;
        Gc.set gc_control;
    | _ -> assert false in
  let oc = Daemon.cast_out oc in
  if !Utils.profile then begin
    let f = open_out (string_of_int (Unix.getpid ())^".log") in
    Utils.log := (fun s -> Printf.fprintf f "%s\n" s)
  end;
  (* And now start the daemon worker *)
  try
    while true do
      (* This is a trick to use less memory and to be faster.
       * If we fork now, the heap is very small, because no job
       * was sent in yet.
      *)
      let in_fd = Daemon.descr_of_in_channel ic in
      let readyl, _, _ = Unix.select [in_fd] [] [] (-1.0) in
      if readyl = [] then exit 0;
      match Fork.fork() with
      | 0 -> slave_main ic oc
      | pid ->
          match snd (Unix.waitpid [] pid) with
          | Unix.WEXITED 0 -> ()
          | Unix.WEXITED 1 ->
              raise End_of_file
          | Unix.WEXITED x ->
              Printf.printf "Worker exited (code: %d)\n" x;
              flush stdout;
              raise End_of_file
          | Unix.WSIGNALED x ->
              let sig_str = PrintSignal.string_of_signal x in
              Printf.printf "Worker interrupted with signal: %s\n" sig_str;
              exit 2
          | Unix.WSTOPPED x ->
              Printf.printf "Worker stopped with signal: %d\n" x;
              exit 3
    done;
    assert false
  with End_of_file -> exit 0


(*****************************************************************************)
(* Creates a pool of workers. *)
(*****************************************************************************)

let workers = ref []

let win32_slave_entry =
  Daemon.register_entry_point
    "win32_slave"
    win32_slave_main

let linux_slave_entry =
  Daemon.register_entry_point
    "linux_slave"
    linux_slave_main

let do_make id gc_control heap_handle =
  let preforked =
    if Sys.win32 then begin
      None
    end else begin
      let { Daemon.channels = (_, outc); _ } as handle =
        Daemon.spawn ~reason:"worker" linux_slave_entry in
      Daemon.to_channel outc (Init (gc_control, heap_handle));
      Some handle
    end in
  let worker =
    { id; job = NoJob; heap_handle; preforked; killed = false; } in
  workers := worker :: !workers;
  worker

let make_one =
  let cpt = ref 0 in
  fun control heap_handle ->
    let id = !cpt in
    if id >= max_workers then failwith "Too many workers";
    incr cpt;
    do_make id control heap_handle

let rec make n control heap_handle =
  if n <= 0 then
    []
  else
    let worker = make_one control heap_handle in
    worker :: make (pred n) control heap_handle

(* A function that will be 'marshalled' to the worker. *)
let worker_fun f x restore_state = fun send_result ->
  (* 'f',  'x', and others arguments are captured in the closure. *)
  restore_state ();
  send_result (f x)

let call w (type a) (type b) (f : a -> b) (x : a) : b handle =
  if w.killed then Printf.ksprintf failwith "killed worker (%d)" w.id;
  match w.job with
  | Job _ -> Printf.ksprintf failwith "busy worker (%d)" w.id
  | NoJob ->
      let { Daemon.pid = slave_pid; channels = (inc, outc) } as h =
        match w.preforked with
        | None ->
            assert (Sys.win32);
            Daemon.spawn win32_slave_entry
        | Some handle ->
            assert (not Sys.win32);
            handle in
      (* Prepare ourself to read answer from the slave,
         and mark the worker as busy. *)
      let infd = Daemon.descr_of_in_channel inc in
      let rec result () : b =
        let _, status = Unix.waitpid [Unix.WNOHANG] slave_pid in
        match status with
        | Unix.WEXITED 0 ->
            let res : b = Marshal.from_channel (Daemon.cast_in inc) in
            if w.preforked = None then Daemon.close h;
            res
        | Unix.WEXITED i ->
            Printf.ksprintf failwith "Subprocess(%d): fail %d" slave_pid i
        | _ ->
            Printf.ksprintf failwith "Subprocess(%d): fail" slave_pid
      and slave = { result; slave_pid; infd; worker = w; } in
      w.job <- Job slave;
      let restore_state =
        if Sys.win32 then
          let heap_handle = w.heap_handle in
          let saved_prefix = Relative_path.save () in
          fun () ->
            SharedMem.connect heap_handle;
            Relative_path.restore saved_prefix
        else
          (fun () -> ()) in
      (* Send the job to the slave. *)
      Daemon.to_channel outc
        ~flush:true ~flags:[Marshal.Closures]
        (Request (worker_fun f x restore_state));
      (* And returned the 'handle'. *)
      ref (Processing slave)

let get_result d =
  match !d with
  | Cached x -> x
  | Failed exn -> raise exn
  | Processing s ->
      try
        let res = s.result () in
        s.worker.job <- NoJob;
        d := Cached res;
        res
      with exn ->
        s.worker.job <- NoJob;
        d := Failed exn;
        raise exn

let get_worker h =
  match !h with
  | Processing {worker; _} -> worker
  | Cached _
  | Failed _ -> invalid_arg "Worker.get_worker"

let kill w =
  if not w.killed then begin
    w.killed <- true;
    match w.preforked with
    | None -> ()
    | Some handle -> Daemon.kill handle
  end

let killall () =
  List.iter ~f:kill !workers
