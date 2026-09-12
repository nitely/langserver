import
  tsuggestapi,
  tnimlangserver

# XXX Fix getNimPath not finding nim on windows
when not defined(windows):
  import
    tnimtrack

import
  tprojectsetup,
  tmisc,
  ttestrunner,
  tmcp

# https://github.com/nim-lang/langserver/pull/451
import
  textensions
