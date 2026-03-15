#!/usr/bin/env python3
"""
机器人任务调度器
Robot Task Scheduler

从 JSON 文件加载任务，解析依赖关系，按序执行原子动作
"""

import json
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class AtomicAction:
    id: int
    skill: str
    params: Dict[str, Any]
    depends_on: List[int] = None
    timeout: int = 30
    status: str = "pending"
    result: Optional[Dict] = None
    error: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    
    def __post_init__(self):
        if self.depends_on is None:
            self.depends_on = []

@dataclass
class Task:
    task_id: str
    description: str
    robot: str
    actions: List[AtomicAction]
    status: str = "pending"
    created_at: str = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()

class RobotScheduler:
    """任务调度器"""
    
    def __init__(self, skill_library_path: str):
        with open(skill_library_path, 'r', encoding='utf-8') as f:
            self.skill_lib = json.load(f)
        
        self.atomic_skills = self.skill_lib.get('atomic_skills', {})
        self.composite_skills = self.skill_lib.get('composite_skills', {})
        
        # 模拟机器人 API
        self.robot_api = MockRobotAPI()
        
        # 任务历史
        self.task_history: List[Task] = []
    
    def parse_task_from_json(self, task_json: str) -> Task:
        """从 JSON 解析任务"""
        data = json.loads(task_json)
        
        actions = []
        for a in data.get('actions', []):
            actions.append(AtomicAction(
                id=a['id'],
                skill=a['atomic'],
                params=a.get('params', {}),
                depends_on=a.get('depends_on', []),
                timeout=a.get('timeout', 30)
            ))
        
        return Task(
            task_id=data.get('task_id', 'unknown'),
            description=data.get('description', ''),
            robot=data.get('robot', 'Unitree G1'),
            actions=actions
        )
    
    def load_task_from_file(self, filepath: str) -> Task:
        """从文件加载任务"""
        with open(filepath, 'r', encoding='utf-8') as f:
            return self.parse_task_from_json(f.read())
    
    async def execute_action(self, action: AtomicAction) -> Dict:
        """执行单个原子动作"""
        print(f"  ▶ 执行 Action {action.id}: {action.skill}")
        
        if action.skill not in self.atomic_skills:
            raise ValueError(f"未知技能: {action.skill}")
        
        skill_info = self.atomic_skills[action.skill]
        
        # 模拟调用机器人 API
        result = await self.robot_api.call_skill(
            action.skill, 
            action.params,
            skill_info
        )
        
        return result
    
    async def execute_task(self, task: Task) -> Task:
        """执行完整任务"""
        print(f"\n🚀 开始任务: {task.task_id}")
        print(f"📋 描述: {task.description}")
        print(f"🤖 机器人: {task.robot}")
        print(f"📊 动作数: {len(task.actions)}\n")
        
        task.status = "running"
        completed_ids = set()
        failed = False
        
        # 按依赖顺序执行
        while len(completed_ids) < len(task.actions) and not failed:
            # 找到可以执行的动作（依赖都已完成）
            ready_actions = [
                a for a in task.actions 
                if a.id not in completed_ids 
                and all(dep in completed_ids for dep in a.depends_on)
                and a.status != "skipped"
            ]
            
            if not ready_actions:
                if len(completed_ids) < len(task.actions):
                    print("❌ 死锁：无法满足依赖关系")
                    task.status = "failed"
                    failed = True
                break
            
            # 执行准备好的动作
            for action in ready_actions:
                action.status = "running"
                action.start_time = datetime.now().isoformat()
                
                try:
                    result = await asyncio.wait_for(
                        self.execute_action(action),
                        timeout=action.timeout
                    )
                    action.result = result
                    action.status = "success"
                    print(f"  ✅ Action {action.id} 完成\n")
                    
                except asyncio.TimeoutError:
                    action.status = "failed"
                    action.error = "超时"
                    print(f"  ❌ Action {action.id} 超时\n")
                    failed = True
                    break
                    
                except Exception as e:
                    action.status = "failed"
                    action.error = str(e)
                    print(f"  ❌ Action {action.id} 失败: {e}\n")
                    failed = True
                    break
                
                completed_ids.add(action.id)
                action.end_time = datetime.now().isoformat()
        
        task.status = "success" if not failed else "failed"
        self.task_history.append(task)
        
        print(f"\n🎉 任务{'成功' if task.status == 'success' else '失败'}: {task.task_id}")
        return task
    
    def generate_task_json(self, task_description: str, robot: str = "Unitree G1") -> str:
        """
        AI 自动解析任务描述，生成任务 JSON
        这是一个简化版本，实际可以调用 LLM 来解析
        """
        # 简化规则匹配
        task_lower = task_description.lower()
        
        if "迎接" in task_description or "接待" in task_description:
            template = {
                "task_id": f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "description": task_description,
                "robot": robot,
                "actions": [
                    {"id": 1, "atomic": "locate", "params": {"target": "guest"}},
                    {"id": 2, "atomic": "walk_to", "params": {"x": 1.0, "y": 0.0, "theta": 0}, "depends_on": [1]},
                    {"id": 3, "atomic": "look_at", "params": {"target": "guest"}, "depends_on": [2]},
                    {"id": 4, "atomic": "speak", "params": {"text": "你好，欢迎光临", "emotion": "happy"}, "depends_on": [3]}
                ]
            }
        elif "拿" in task_description or "取" in task_description:
            template = {
                "task_id": f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "description": task_description,
                "robot": robot,
                "actions": [
                    {"id": 1, "atomic": "locate", "params": {"target": task_description.split("拿")[1].split("给")[0].strip() if "给" in task_description else "物品"}},
                    {"id": 2, "atomic": "walk_to", "params": {"x": 1.0, "y": 0.0, "theta": 0}, "depends_on": [1]},
                    {"id": 3, "atomic": "grasp", "params": {"object_id": "item_1", "hand": "right"}, "depends_on": [2]},
                    {"id": 4, "atomic": "walk_to", "params": {"x": 0.0, "y": 0.0, "theta": 180}, "depends_on": [3]},
                    {"id": 5, "atomic": "handover", "params": {"hand": "right"}, "depends_on": [4]}
                ]
            }
        else:
            template = {
                "task_id": f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "description": task_description,
                "robot": robot,
                "actions": [
                    {"id": 1, "atomic": "speak", "params": {"text": f"收到任务：{task_description}"}}
                ]
            }
        
        return json.dumps(template, ensure_ascii=False, indent=2)
    
    def export_to_visualizer(self, task: Task) -> Dict:
        """导出任务数据用于可视化"""
        nodes = []
        edges = []
        
        for action in task.actions:
            skill_info = self.atomic_skills.get(action.skill, {})
            nodes.append({
                "id": str(action.id),
                "label": f"{action.id}. {action.skill}",
                "description": skill_info.get('description', ''),
                "status": action.status,
                "params": action.params,
                "result": action.result,
                "error": action.error
            })
            
            for dep in action.depends_on:
                edges.append({
                    "from": str(dep),
                    "to": str(action.id)
                })
        
        return {
            "task_id": task.task_id,
            "description": task.description,
            "robot": task.robot,
            "status": task.status,
            "nodes": nodes,
            "edges": edges
        }


class MockRobotAPI:
    """模拟机器人 API"""
    
    async def call_skill(self, skill: str, params: Dict, skill_info: Dict) -> Dict:
        """模拟调用机器人技能"""
        # 模拟执行时间
        await asyncio.sleep(0.5)
        
        # 根据技能返回模拟结果
        results = {
            "locate": {"position": {"x": 1.0, "y": 0.5, "z": 0.0}, "confidence": 0.95},
            "walk_to": {"status": "success", "distance": 2.5, "time": 3.2},
            "grasp": {"status": "success", "force": 0.5},
            "release": {"status": "success"},
            "look_at": {"status": "success"},
            "speak": {"status": "success", "duration": 2.1},
            "listen": {"status": "success", "text": "好的"},
            "handover": {"status": "success"},
            "detect_obstacle": {"obstacles": []},
            "avoid_obstacle": {"status": "success", "path": [{"x": 0, "y": 0}, {"x": 1, "y": 0.5}]},
            "climb_stairs": {"status": "success", "steps_climbed": 5},
            "open_door": {"status": "success", "door_angle": 90},
            "press_button": {"status": "success"},
            "detect_fall": {"is_fallen": False},
            "stand_up": {"status": "success"},
            "charge": {"status": "charging"}
        }
        
        return results.get(skill, {"status": "success"})


async def main():
    """测试运行"""
    scheduler = RobotScheduler('skills/library.json')
    
    # 示例任务
    task_json = '''
    {
        "task_id": "test_greet",
        "description": "测试迎接客人任务",
        "robot": "Unitree G1",
        "actions": [
            {"id": 1, "atomic": "locate", "params": {"target": "guest"}},
            {"id": 2, "atomic": "walk_to", "params": {"x": 1.5, "y": 0.0, "theta": 0}, "depends_on": [1]},
            {"id": 3, "atomic": "look_at", "params": {"target": "guest"}, "depends_on": [2]},
            {"id": 4, "atomic": "speak", "params": {"text": "你好，欢迎光临！", "emotion": "happy"}, "depends_on": [3]}
        ]
    }
    '''
    
    task = scheduler.parse_task_from_json(task_json)
    result = await scheduler.execute_task(task)
    
    # 导出可视化数据
    viz_data = scheduler.export_to_visualizer(result)
    with open('visualizer/data.json', 'w', encoding='utf-8') as f:
        json.dump(viz_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n📊 可视化数据已导出到 visualizer/data.json")


if __name__ == "__main__":
    asyncio.run(main())
