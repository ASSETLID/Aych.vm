(**
 * Copyright (c) 2015, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the "hack" directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 *
 *)

type 'a in_channel = Pervasives.in_channel
type 'a out_channel = Pervasives.out_channel

type ('in_, 'out) channel_pair = 'in_ in_channel * 'out out_channel

type ('in_, 'out) handle = {
  channels : ('in_, 'out) channel_pair;
  pid : int;
}

let to_channel :
  'a out_channel -> ?flags:Marshal.extern_flags list -> ?flush:bool ->
  'a -> unit =
  fun oc ?(flags = []) ?flush:(should_flush=true) v ->
    Marshal.to_channel oc v flags;
    if should_flush then flush oc

let from_channel : 'a in_channel -> 'a = fun ic ->
  Marshal.from_channel ic

let flush : 'a out_channel -> unit = Pervasives.flush

let descr_of_in_channel : 'a in_channel -> Unix.file_descr =
  Unix.descr_of_in_channel

let descr_of_out_channel : 'a out_channel -> Unix.file_descr =
  Unix.descr_of_out_channel

type ('a, 'b) entry = Entry of string


(** *)

let fds = ref []
let set_close_on_spawn fd =
  Unix.set_close_on_exec fd;
  if not Sys.win32 then
    fds := fd :: List.filter (fun fd' -> fd' != fd) !fds
let clear_close_on_spawn fd =
  fds := List.filter (fun fd' -> fd' != fd) !fds
let close_fds () =
  if not Sys.win32 then begin
    List.iter Unix.close !fds;
    fds := [];
  end

(** *)

let entry_points :
  (string, (Pervasives.in_channel * Pervasives.out_channel) -> unit)
    Hashtbl.t =
  Hashtbl.create 23
let exec (Entry name) ic oc =
  let f =
    try Hashtbl.find entry_points name
    with Not_found ->
      Printf.ksprintf failwith
        "Unkmown entry point %S" name in
  try f (ic, oc); exit 0
  with _ -> exit 1

let register_entry_point name f =
  if Hashtbl.mem entry_points name then
    Printf.ksprintf failwith
      "Daemon.register_entry_point: duplicate entry point %S."
      name;
  Hashtbl.add entry_points name f;
  Entry name

let null_path = Path.to_string Path.null_path

let make_pipe () =
  let descr_in, descr_out = Unix.pipe () in
  (* close descriptors on exec so they are not leaked *)
  Unix.set_close_on_exec descr_in;
  Unix.set_close_on_exec descr_out;
  let ic = Unix.in_channel_of_descr descr_in in
  let oc = Unix.out_channel_of_descr descr_out in
  ic, oc

let fork ?log_file (f : ('a, 'b) channel_pair -> unit) :
    ('b, 'a) handle =
  let parent_in, child_out = make_pipe () in
  let child_in, parent_out = make_pipe () in
  match Fork.fork () with
  | -1 -> failwith "Go get yourself a real computer"
  | 0 -> (* child *)
      close_in parent_in;
      close_out parent_out;
      Sys_utils.with_umask 0o111 begin fun () ->
        let fd =
          Unix.openfile null_path [Unix.O_RDONLY; Unix.O_CREAT] 0o777 in
        Unix.dup2 fd Unix.stdin;
        Unix.close fd;
        let fn = Option.value_map log_file ~default:null_path ~f:
          begin fun fn ->
            Sys_utils.mkdir_no_fail (Filename.dirname fn);
            fn
          end in
        let fd = Unix.openfile fn [Unix.O_WRONLY; Unix.O_CREAT] 0o666 in
        Unix.dup2 fd Unix.stdout;
        Unix.dup2 fd Unix.stderr;
        Unix.close fd;
      end;
      f (child_in, child_out);
      exit 0
  | pid -> (* parent *)
      close_in child_in;
      close_out child_out;
      { channels = parent_in, parent_out; pid }

let unix_spawn
    ?reason
    ?log_file
    (entry: ('a, 'b) entry) : ('b, 'a) handle =
  let parent_in, child_out = Unix.pipe () in
  let child_in, parent_out = Unix.pipe () in
  (* Close descriptors on 'spawn' so they are not leaked. *)
  set_close_on_spawn parent_in;
  set_close_on_spawn parent_out;
  match Fork.fork_and_may_log ?reason () with
  | -1 -> failwith "Go get yourself a real computer"
  | 0 -> (* child *)
      close_fds ();
      set_close_on_spawn child_in;
      set_close_on_spawn child_out;
      Sys_utils.with_umask 0o111 begin fun () ->
        let fd =
          Unix.openfile null_path [Unix.O_RDONLY; Unix.O_CREAT] 0o777 in
        Unix.dup2 fd Unix.stdin;
        Unix.close fd;
        let fn = Option.value_map log_file ~default:"/dev/null" ~f:
          begin fun fn ->
            Sys_utils.mkdir_no_fail (Filename.dirname fn);
            fn
          end in
        let fd =
          Unix.openfile fn [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC] 0o666 in
        Unix.dup2 fd Unix.stdout;
        Unix.dup2 fd Unix.stderr;
        Unix.close fd;
      end;
      exec entry
        (Unix.in_channel_of_descr child_in)
        (Unix.out_channel_of_descr child_out)
  | pid -> (* parent *)
      Unix.close child_in;
      Unix.close child_out;
      { channels = Unix.in_channel_of_descr parent_in,
                   Unix.out_channel_of_descr parent_out;
        pid }

let win32_spawn
    ?reason ?log_file (Entry entry: ('a, 'b) entry) : ('b, 'a) handle =
  let parent_in, child_out = Unix.pipe () in
  let child_in, parent_out = Unix.pipe () in
  (* Close descriptors on exec so they are not leaked. *)
  Unix.set_close_on_exec parent_in;
  Unix.set_close_on_exec parent_out;
  Unix.putenv "HH_SERVER_DAEMON" entry;
  Unix.putenv "HH_SERVER_DAEMON_IN"
    (string_of_int (Handle.get_handle child_in));
  Unix.putenv "HH_SERVER_DAEMON_OUT"
    (string_of_int (Handle.get_handle child_out));
  let null_fd =
    Unix.openfile null_path [Unix.O_RDONLY; Unix.O_CREAT] 0o777 in
  let out_path =
    Option.value_map log_file
      ~default:null_path
      ~f:(fun fn ->
          Sys_utils.mkdir_no_fail (Filename.dirname fn);
          fn)  in
  let out_fd =
    Unix.openfile out_path [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC] 0o666 in
  let pid =
    Unix.create_process
      Sys.executable_name [|Sys.executable_name|]
      null_fd out_fd out_fd in
  Option.iter reason ~f:(fun reason -> PidLog.log ~reason pid);
  Unix.close child_in;
  Unix.close child_out;
  Unix.close out_fd;
  Unix.close null_fd;
  { channels = Unix.in_channel_of_descr parent_in,
               Unix.out_channel_of_descr parent_out;
    pid }

let spawn = if Sys.win32 then win32_spawn else unix_spawn

(* for testing code *)
let devnull () =
  let ic = open_in "/dev/null" in
  let oc = open_out "/dev/null" in
  {channels = ic, oc; pid = 0}

let check_entry_point () =
  try
    let entry = Entry (Unix.getenv "HH_SERVER_DAEMON") in
    let ic =
      try
        Sys.getenv "HH_SERVER_DAEMON_IN"
        |> int_of_string
        |> Handle.wrap_handle
        |> Unix.in_channel_of_descr
      with _ -> failwith "Can't define daemon input." in
    let oc =
      try
        Sys.getenv "HH_SERVER_DAEMON_OUT"
        |> int_of_string
        |> Handle.wrap_handle
        |> Unix.out_channel_of_descr
      with _ -> failwith "Can't define daemon output." in
    exec entry ic oc
  with Not_found -> ()

let close { channels = (ic, oc); _ } =
  clear_close_on_spawn (Unix.descr_of_in_channel ic);
  clear_close_on_spawn (Unix.descr_of_out_channel oc);
  close_in ic;
  close_out oc

let kill h =
  close h;
  Unix.kill h.pid Sys.sigkill


let cast_in x = x
let cast_out x = x
