import re
import pandas as pd
import json
import numpy as np
# import operator
import time
import datetime
from usageMap import UsageMap
from collections import Counter


if __name__ == '__main__':
    with open('./new_data/liangji_group_by_customer') as f:
        for line in f:
            print line
            break
