"""
run_pipeline.py
===============
main.py の薄いラッパー。
後方互換のために残しているが、実体は main.main() を呼ぶだけ。
GitHub Actions の workflow からは main.py を直接呼ぶことを推奨。
"""

from main import main

if __name__ == "__main__":
    main()
