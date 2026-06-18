#!/usr/bin/env python3
"""
Causal Log Analyzer - 主入口
分布式系统链路日志因果链推断工具
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from causal_analyzer.cli import main

if __name__ == "__main__":
    main()
