(**
 * Copyright (c) 2015, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the "hack" directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 *
 *)

(*****************************************************************************)
(* Prelude *)
(*****************************************************************************)

open Core

let fold_files (type t)
    ?max_depth ?(filter=(fun _ -> true))
    (paths: Path.t list) (action: string -> t -> t) (init: t) =
  let rec fold depth acc dir =
    let recurse = max_depth <> Some depth in
    let files = Sys.readdir dir in
    Array.fold_left
      (fun acc file ->
         let open Unix in
         let file = Filename.concat dir file in
         match (lstat file).st_kind with
         | S_REG when filter file -> action file acc
         | S_DIR when recurse -> fold (depth+1) acc file
         | _ -> acc)
      acc files in
  if max_depth <> Some 0 then
    let paths = List.map paths Path.to_string in
    List.fold_left paths ~init ~f:(fold 1)
  else
    init

let iter_files ?max_depth ?filter paths action =
  fold_files ?max_depth ?filter paths (fun file _ -> action file) ()

let find ?max_depth ?filter paths =
  fold_files ?max_depth ?filter paths List.cons []

(*****************************************************************************)
(* Main entry point *)
(*****************************************************************************)

let make_next_files ?filter ?(others=[]) root =
  let done_ = ref false in
  fun () ->
    if !done_ then
      (* see multiWorker.mli, this is the protocol for nextfunc *)
      []
    else
      (done_ := true; find ?filter (root :: others))

