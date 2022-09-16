from toysql.planner import Program, Opcode
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
        assert len(program.instructions)

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
                # Read column at index p2 and store in register p3
                row = btrees[instruction.p1].peek()
                registers[instruction.p2] = row.row_id
                cursor += 1

            if instruction.opcode == Opcode.Column:
                # Read column at index p2 and store in register p3
                row = btrees[instruction.p1].peek()
                v = row.values[instruction.p2][1]

                registers[instruction.p3] = v
                cursor += 1

            if instruction.opcode == Opcode.ResultRow:
                # Take all the stored values in registers p1 - p2 and yeild them
                # to the caller.

                values = []
                for i in range(
                    cast(int, instruction.p1), cast(int, instruction.p2) + 1
                ):
                    values.append(registers[i])

                cursor += 1

                yield values

            if instruction.opcode == Opcode.Next:
                # Check if btree cursor p1 has next value.
                # If next continue to address p2
                # else fall through to next instruction.
                next(btrees[instruction.p1])
                v = btrees[instruction.p1].peek()

                if v is not None:
                    cursor = cast(int, instruction.p2)
                else:
                    cursor += 1

            if instruction.opcode == Opcode.Halt:
                # Immediate exit.
                break

        return
