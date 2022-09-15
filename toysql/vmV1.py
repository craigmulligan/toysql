from toysql.planner import Program, Opcode
from toysql.table import Table
from toysql.btree import BTree
from typing import cast
from collections import deque


class PeekIterator:
    def __init__(self, iterable):
        self.iterator = iter(iterable)
        self.peeked = deque()

    def __iter__(self):
        return self

    def __next__(self):
        if self.peeked:
            return self.peeked.popleft()

        return self.safe_next()

    def safe_next(self):
        try:
            return next(self.iterator)
        except StopIteration:
            return None

    def peek(self, ahead=0):
        while len(self.peeked) <= ahead:
            v = self.safe_next()
            self.peeked.append(v)
        return self.peeked[ahead]


class VM:
    def __init__(self, pager):
        self.pager = pager

    def execute(self, program: Program):
        btrees = {}
        registers = {}
        cursor = 0
        while True:
            instruction = program.instructions[cursor]

            if instruction.opcode == Opcode.Init:
                # init instruction tells us which address to start at.
                cursor = cast(int, instruction.p2)

            if instruction.opcode == Opcode.Transaction:
                # No support for Transactions yet.
                cursor += 1

            if instruction.opcode == Opcode.Goto:
                # Unconditional jump to instruction at address p2
                cursor = cast(int, instruction.p2)

            if instruction.opcode == Opcode.OpenRead:
                # Open a cursor with root page p2 and assign its refname to val p1
                btrees[instruction.p1] = PeekIterator(
                    BTree(self.pager, instruction.p2).scan()
                )
                cursor += 1

            if instruction.opcode == Opcode.Rewind:
                # If table or index is empty jump to p2
                if not btrees[instruction.p1].peek():
                    cursor = cast(int, instruction.p2)
                else:
                    cursor += 1

            if instruction.opcode == Opcode.Rowid:
                row = btrees[instruction.p1].peek()

                if row is None:
                    # This should never happen.
                    raise Exception("record not found")
                registers[instruction.p2] = row.row_id
                cursor += 1

            if instruction.opcode == Opcode.Column:
                # Extra column at index p2 and store in register p3
                row = btrees[instruction.p1].peek()

                if row is None:
                    # This should never happen.
                    raise Exception("record not found")

                registers[instruction.p3] = row.values[instruction.p2][1]
                cursor += 1

            if instruction.opcode == Opcode.ResultRow:
                values = []
                for i in range(cast(int, instruction.p1), cast(int, instruction.p2)):
                    values.append(registers[i])

                cursor += 1
                yield values

            if instruction.opcode == Opcode.Next:
                next(btrees[instruction.p1])
                v = btrees[instruction.p1].peek()

                if v is not None:
                    cursor = cast(int, instruction.p2)
                else:
                    cursor += 1

            if instruction.opcode == Opcode.Halt:
                break

        return
