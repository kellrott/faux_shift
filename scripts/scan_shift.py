#!/usr/bin/env python

import os
from glob import glob
import sys
import json
import math


if __name__ == "__main__":
    for a in glob(os.path.join(sys.argv[1], "part-*")):
        with open(a) as handle:
            for line in handle:
                rec = json.loads(line)

                for sample in rec['shift']:
                    non_zero = list( (k,v) for k,v in rec['shift'][sample].items() if not math.isnan(v) and v != 0.0 )
                    non_zero = sorted(non_zero, key=lambda x:abs(x[1]), reverse=True)
                    print sample, rec['gene'], "\t".join( "%s:%s" % (k,v) for k,v in non_zero )
