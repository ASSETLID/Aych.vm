(**
 * Copyright (c) 2014, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the "hack" directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 *
 *)

open Utils

type t = (SSet.t, DfindServer.r) Daemon.handle

let init roots =
  let {Daemon.channels = (ic, oc); pid = _} as handle =
    Daemon.spawn DfindServer.entry_point in
  Daemon.to_channel oc (DfindServer.Init roots);
  handle

let pid handle = handle.Daemon.pid

let request_changes {Daemon.channels = (ic, oc); pid = _} =
  Daemon.to_channel oc DfindServer.Request;
  Daemon.from_channel ic

let get_changes daemon =
  let rec loop acc =
    let diff = request_changes daemon in
    if SSet.is_empty diff
    then acc
    else begin
      let acc = SSet.union diff acc in
      loop acc
    end
  in loop SSet.empty
