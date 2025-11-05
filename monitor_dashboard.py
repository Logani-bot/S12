#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
실시간 감시 프로그램 모니터링 대시보드

현재 실행중인 감시 프로그램들의 상태를 확인하고 제어합니다.
- 프로세스 상태 확인 (실행중/정지)
- 원클릭 시작/재시작
- 로그 파일 마지막 업데이트 시간 확인
"""

import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import time
import json
from typing import Dict, List, Optional, Tuple
import re

# Windows 콘솔 인코딩 설정
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 모니터링 대상 프로그램 설정
MONITORED_PROGRAMS = [
    {
        "name": "S12 주식 실시간 모니터",
        "script": "Real_Time_Monitor.py",
        "bat_file": "run_real_time_monitor.bat",
        "log_pattern": "realtime_monitor_*.log",
        "description": "거래일 08:00-20:00, 10분 간격",
        "enabled": True
    },
    {
        "name": "업비트 시장 하락 감시",
        "script": "upbit_alert_optimized.py",
        "bat_file": "run_upbit_optimized.bat",
        "log_pattern": "upbit_alert_*.log",
        "description": "평일 09:00-18:00, 30분 간격",
        "enabled": True
    },
    {
        "name": "암호화폐 실시간 모니터",
        "script": "crypto_realtime_monitor.py",
        "bat_file": None,  # bat 파일이 없는 경우
        "log_pattern": "crypto_monitor_*.log",
        "description": "00:00 파일생성, 30분 간격 알람",
        "enabled": True
    }
]

class MonitorDashboard:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.status_cache_file = self.base_dir / "monitor_status_cache.json"

    def find_process(self, script_name: str) -> Optional[Dict]:
        """특정 Python 스크립트를 실행하는 프로세스 찾기 (Windows tasklist 사용)"""
        try:
            # tasklist로 실행중인 프로세스 확인
            result = subprocess.run(
                ['tasklist', '/FI', 'IMAGENAME eq python.exe', '/FO', 'CSV', '/V'],
                capture_output=True,
                text=True,
                encoding='cp949'  # Windows 한글 인코딩
            )

            if result.returncode != 0:
                return None

            # CSV 출력 파싱
            lines = result.stdout.strip().split('\n')
            if len(lines) < 2:  # 헤더만 있거나 결과 없음
                return None

            # 각 프로세스의 명령줄 확인
            for line in lines[1:]:  # 헤더 제외
                # CSV 파싱 (따옴표로 감싸진 필드)
                parts = re.findall(r'"([^"]*)"', line)
                if len(parts) >= 2:
                    image_name = parts[0]
                    pid = parts[1]
                    window_title = parts[-1] if len(parts) > 8 else ""

                    # 스크립트 이름이 윈도우 타이틀이나 명령줄에 포함되어 있는지 확인
                    if script_name.lower() in window_title.lower():
                        return {"pid": int(pid), "name": image_name}

            # tasklist로 찾지 못하면 wmic으로 명령줄 확인
            result = subprocess.run(
                ['wmic', 'process', 'where', 'name="python.exe"', 'get', 'ProcessId,CommandLine', '/format:csv'],
                capture_output=True,
                text=True,
                encoding='cp949',
                timeout=5
            )

            if result.returncode == 0:
                for line in result.stdout.strip().split('\n')[1:]:  # 헤더 제외
                    if script_name in line:
                        # CSV 형식: Node,CommandLine,ProcessId
                        parts = line.split(',')
                        if len(parts) >= 3:
                            try:
                                pid = int(parts[-1].strip())
                                return {"pid": pid, "name": "python.exe"}
                            except ValueError:
                                pass
        except Exception as e:
            # wmic나 tasklist가 실패해도 무시
            pass

        return None

    def get_latest_log_file(self, pattern: str) -> Optional[Path]:
        """패턴에 맞는 가장 최근 로그 파일 찾기"""
        try:
            import glob
            log_files = list(self.base_dir.glob(pattern))
            if log_files:
                return max(log_files, key=lambda p: p.stat().st_mtime)
        except Exception:
            pass
        return None

    def get_log_status(self, log_file: Path) -> Tuple[str, str]:
        """로그 파일 상태 확인"""
        if not log_file or not log_file.exists():
            return "❌", "로그 없음"

        try:
            # 마지막 수정 시간
            mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
            now = datetime.now()
            delta = now - mtime

            # 5분 이내: 활성, 1시간 이내: 정상, 그 이상: 오래됨
            if delta < timedelta(minutes=5):
                status = "🟢"
                time_str = f"{int(delta.total_seconds())}초 전"
            elif delta < timedelta(hours=1):
                status = "🟡"
                time_str = f"{int(delta.total_seconds() / 60)}분 전"
            else:
                status = "🟠"
                time_str = mtime.strftime("%H:%M")

            return status, time_str
        except Exception:
            return "❓", "확인 불가"

    def check_program_status(self, program: Dict) -> Dict:
        """개별 프로그램 상태 확인"""
        if not program["enabled"]:
            return {
                "running": False,
                "status": "⚪",
                "status_text": "비활성화",
                "pid": None,
                "log_status": "❌",
                "log_time": "N/A"
            }

        # 프로세스 확인
        proc = self.find_process(program["script"])
        running = proc is not None
        pid = proc["pid"] if proc else None

        # 로그 확인
        log_file = self.get_latest_log_file(program["log_pattern"])
        log_status, log_time = self.get_log_status(log_file)

        # 종합 상태
        if running:
            status = "🟢"
            status_text = "실행중"
        else:
            status = "🔴"
            status_text = "정지"

        return {
            "running": running,
            "status": status,
            "status_text": status_text,
            "pid": pid,
            "log_status": log_status,
            "log_time": log_time
        }

    def start_program(self, program: Dict) -> bool:
        """프로그램 시작"""
        try:
            # bat 파일이 있으면 bat 파일로 실행
            if program["bat_file"]:
                bat_path = self.base_dir / program["bat_file"]
                if bat_path.exists():
                    subprocess.Popen([str(bat_path)],
                                   shell=True,
                                   creationflags=subprocess.CREATE_NEW_CONSOLE)
                    time.sleep(2)  # 프로세스 시작 대기
                    return True

            # bat 파일이 없으면 직접 Python 스크립트 실행
            script_path = self.base_dir / program["script"]
            if script_path.exists():
                # Python 경로 찾기
                python_exe = sys.executable
                subprocess.Popen([python_exe, str(script_path)],
                               creationflags=subprocess.CREATE_NEW_CONSOLE)
                time.sleep(2)
                return True

        except Exception as e:
            print(f"❌ 시작 실패: {e}")
        return False

    def stop_program(self, program: Dict) -> bool:
        """프로그램 정지"""
        try:
            proc = self.find_process(program["script"])
            if proc:
                pid = proc["pid"]
                # taskkill로 프로세스 종료
                subprocess.run(['taskkill', '/F', '/PID', str(pid)],
                             capture_output=True)
                time.sleep(1)
                return True
        except Exception as e:
            print(f"❌ 정지 실패: {e}")
        return False

    def display_dashboard(self):
        """대시보드 출력"""
        # 화면 지우기
        os.system('cls' if os.name == 'nt' else 'clear')

        print("=" * 80)
        print("🖥️  실시간 감시 프로그램 모니터링 대시보드")
        print("=" * 80)
        print(f"⏰ 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        print()

        # 각 프로그램 상태 확인 및 출력
        statuses = []
        for idx, program in enumerate(MONITORED_PROGRAMS, 1):
            status = self.check_program_status(program)
            statuses.append((program, status))

            print(f"{idx}. {program['name']}")
            print(f"   상태: {status['status']} {status['status_text']}", end="")
            if status['pid']:
                print(f" (PID: {status['pid']})", end="")
            print()
            print(f"   설명: {program['description']}")
            print(f"   로그: {status['log_status']} {status['log_time']}")
            print()

        print("=" * 80)
        print("명령어:")
        print("  [1-9]    : 해당 번호 프로그램 시작/재시작")
        print("  s[1-9]   : 해당 번호 프로그램 정지")
        print("  a        : 모든 프로그램 시작")
        print("  x        : 모든 프로그램 정지")
        print("  r        : 화면 새로고침")
        print("  q        : 종료")
        print("=" * 80)

        return statuses

    def handle_command(self, cmd: str, statuses: List) -> bool:
        """사용자 명령 처리"""
        cmd = cmd.strip().lower()

        if cmd == 'q':
            return False

        elif cmd == 'r':
            return True

        elif cmd == 'a':
            print("\n🚀 모든 프로그램 시작 중...")
            for program, status in statuses:
                if program["enabled"] and not status["running"]:
                    print(f"  ▶️ {program['name']} 시작 중...")
                    if self.start_program(program):
                        print(f"  ✅ {program['name']} 시작됨")
                    else:
                        print(f"  ❌ {program['name']} 시작 실패")
            time.sleep(2)
            return True

        elif cmd == 'x':
            print("\n🛑 모든 프로그램 정지 중...")
            for program, status in statuses:
                if status["running"]:
                    print(f"  ⏹️ {program['name']} 정지 중...")
                    if self.stop_program(program):
                        print(f"  ✅ {program['name']} 정지됨")
                    else:
                        print(f"  ❌ {program['name']} 정지 실패")
            time.sleep(2)
            return True

        elif cmd.startswith('s') and len(cmd) > 1:
            # 정지 명령
            try:
                idx = int(cmd[1:]) - 1
                if 0 <= idx < len(statuses):
                    program, status = statuses[idx]
                    if status["running"]:
                        print(f"\n⏹️ {program['name']} 정지 중...")
                        if self.stop_program(program):
                            print(f"✅ {program['name']} 정지됨")
                        else:
                            print(f"❌ {program['name']} 정지 실패")
                    else:
                        print(f"\n⚠️ {program['name']}는 이미 정지되어 있습니다.")
                    time.sleep(2)
                    return True
            except ValueError:
                pass

        elif cmd.isdigit():
            # 시작/재시작 명령
            idx = int(cmd) - 1
            if 0 <= idx < len(statuses):
                program, status = statuses[idx]

                # 이미 실행중이면 재시작 확인
                if status["running"]:
                    print(f"\n⚠️ {program['name']}는 이미 실행중입니다.")
                    confirm = input("재시작하시겠습니까? (y/n): ").strip().lower()
                    if confirm == 'y':
                        print(f"⏹️ {program['name']} 정지 중...")
                        self.stop_program(program)
                        time.sleep(1)

                print(f"🚀 {program['name']} 시작 중...")
                if self.start_program(program):
                    print(f"✅ {program['name']} 시작됨")
                else:
                    print(f"❌ {program['name']} 시작 실패")
                time.sleep(2)
                return True

        print("\n❌ 알 수 없는 명령입니다.")
        time.sleep(1)
        return True

    def run(self):
        """대시보드 메인 루프"""
        print("🖥️  모니터링 대시보드를 시작합니다...")
        time.sleep(1)

        try:
            while True:
                statuses = self.display_dashboard()
                cmd = input("\n명령 입력: ")
                if not self.handle_command(cmd, statuses):
                    break
        except KeyboardInterrupt:
            print("\n\n👋 대시보드를 종료합니다.")
        except Exception as e:
            print(f"\n❌ 오류 발생: {e}")
            import traceback
            traceback.print_exc()

def main():
    """메인 함수"""
    dashboard = MonitorDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()
