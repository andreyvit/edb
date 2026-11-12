Copied from Go 1.27.3, src/html/template (BSD-3-Clause; see LICENSE).

Changes: package name; removed internal/godebug dependency and permanently enabled
the secure meta-refresh URL escaping default. planner.go exposes compile-time
context analysis and preselected escaping functions to GoX. Keep upstream files
otherwise unchanged when updating.

Copied escaping, transition and trusted-content tests as well. The test for
disabling meta-refresh URL escaping is omitted because this copy always enables
that protection. Package-qualified error expectations use the local package name.
