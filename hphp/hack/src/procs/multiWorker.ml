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

type 'a nextlist =
  unit -> 'a list

let single_threaded_call job merge neutral next =
  let x = ref (next()) in
  let acc = ref neutral in
  (* This is a just a sanity check that the job is serializable and so
   * that the same code will work both in single threaded and parallel
   * mode.
   *)
  let _ = Marshal.to_string job [Marshal.Closures] in
  while !x != [] do
    acc := job !acc !x;
    x := next()
  done;
  !acc


let multi_threaded_call
  (type a) (type b)
  workers (job: b -> a list -> b)
  (merge: b -> b -> b)
  (neutral: b)
  (next: a nextlist) =
  let rec loop workers handles acc =
    (* 'worker' represents available workers. *)
    (* 'handles' represents pendings jobs. *)
    (* 'acc' are the accumulated results. *)
    match workers with
    | [] when handles = [] -> acc
    | [] ->
        (* No worker available: wait for some workers to finish. *)
        let { Worker.readys; waiters } = Worker.select handles in
        let workers = List.map ~f:Worker.get_worker readys in
        (* Collect there results. *)
        let acc =
          List.fold_left
            ~f:(fun acc h -> merge (Worker.get_result h) acc)
            ~init:acc
            readys in
        (* And continue.. *)
        loop workers waiters acc
    | worker :: workers ->
        (* At least one worker is available... *)
        match next () with
        | [] ->
            (* ... but no more job to be distributed, let's collect results. *)
            loop [] handles acc
        | bucket ->
            (* ... send a job to the worker.*)
            let handle =
              Worker.call worker
                (fun xl -> job neutral xl)
                bucket in
            loop workers (handle :: handles) acc in
  loop workers [] neutral

let call workers ~job ~merge ~neutral ~next =
  match workers with
  | None -> single_threaded_call job merge neutral next
  | Some workers -> multi_threaded_call workers job merge neutral next
