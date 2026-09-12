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

# XXX task* messes the env vars
import
  textensions
