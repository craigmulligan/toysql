from toysql.planner import Program


class VM:
    def __init__(self, pager):
        self.pager = pager

    def execute(self, program: Program):
        for instruction in program.instructions:
            print(instruction)

        return []
