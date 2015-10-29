(**
 * Copyright (c) 2014, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the "hack" directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 *
 *)

type slave_state = string option * string option
let save () =
  Path.make (Relative_path.(path_of_prefix Root)),
  Path.make (Relative_path.(path_of_prefix Hhi))
let restore (saved_root, saved_hhi) =
  HackSearchService.attach_hooks ();
  Relative_path.(set_path_prefix Root saved_root);
  Relative_path.(set_path_prefix Hhi saved_hhi)

let builder =
  Worker.register_state_handler {
    Worker.save = save;
    restore = restore;
  }

let make options config handle =
  let gc_control = ServerConfig.gc_control config in
  let nbr_procs  = GlobalConfig.nbr_procs in
  Some (Worker.make builder nbr_procs gc_control handle)
