from toysql.compiler import Program, Opcode
from toysql.record import DataType, Record
from toysql.btree import BTree, Cursor
from typing import cast


class VM:
    def __init__(self, pager):
        self.pager = pager

    def execute(self, program: Program):
        btrees = {}
        registers = {}
        cursor = 0
        print("\n".join([str(instruct) for instruct in (program.instructions)]))

        while len(program.instructions) > cursor:
            instruction = program.instructions[cursor]
            if instruction.opcode == Opcode.CreateTable:
                # TODO: Should be able to roll this back.
                # RN: pager.new() will write to disk.
                page_number = self.pager.new()
                registers[instruction.p1] = page_number
                cursor += 1

            if instruction.opcode == Opcode.SCopy:
                # shallow copy register value p1 -> p2.
                registers[instruction.p2] = registers[instruction.p1]
                cursor += 1

            if instruction.opcode == Opcode.OpenWrite:
                # Open btree with write cursor (Currently cursors don't have read/write flag)
                # TODO: Also p4 is unimplemeneted.
                root_page_number = registers[instruction.p2]
                btrees[instruction.p1] = Cursor(BTree(self.pager, root_page_number))
                cursor += 1

            if instruction.opcode == Opcode.OpenRead:
                # Open a cursor with root page p2 and assign its refname to val p1
                root_page_number = registers[instruction.p2]
                btrees[instruction.p1] = Cursor(BTree(self.pager, root_page_number))
                cursor += 1

            if instruction.opcode == Opcode.String:
                registers[instruction.p2] = instruction.p4
                cursor += 1

            if instruction.opcode == Opcode.Integer:
                registers[instruction.p2] = instruction.p1
                cursor += 1

            if instruction.opcode == Opcode.Noop:
                cursor += 1

            if instruction.opcode == Opcode.Rewind:
                # If table or index is empty jump to p2
                # else rewind the btree cursor to start.
                tree = btrees[instruction.p1]

                if tree.row_count() == 0:
                    cursor = cast(int, instruction.p2)
                else:
                    btrees[instruction.p1].reset()
                    cursor += 1

            if instruction.opcode == Opcode.Key:
                # Read column at index p2 and store in register p3
                row = btrees[instruction.p1].current()
                registers[instruction.p2] = row.row_id
                cursor += 1

            if instruction.opcode == Opcode.Column:
                # Read column at index p2 and store in register p3
                row = btrees[instruction.p1].current()

                v = row.values[instruction.p2][1]
                registers[instruction.p3] = v
                cursor += 1

            if instruction.opcode == Opcode.MakeRecord:
                values = []

                for i in range(instruction.p2):
                    c = []
                    v = registers[instruction.p1 + i]
                    t = DataType.infer(v)
                    c.append(t)
                    c.append(registers[instruction.p1 + i])
                    values.append(c)

                registers[instruction.p3] = values 
                cursor += 1

            if instruction.opcode == Opcode.ResultRow:
                # Take all the stored values in registers p1 - p2 and yield them
                # to the caller.
                values = []

                for i in range(
                    cast(int, instruction.p1), cast(int, instruction.p2) + 1
                ):
                    values.append(registers[i])

                cursor += 1

                yield values

            if instruction.opcode == Opcode.Insert:
                values = registers[instruction.p2]
                key_with_values = [[DataType.integer, registers[instruction.p3]], *values]

                record = Record(key_with_values)

                btrees[instruction.p1].insert(record)
                registers[instruction.p2] = record
                cursor += 1

            if instruction.opcode == Opcode.Next:
                tree = btrees[instruction.p1]

                try:
                    v = next(tree)
                    cursor = cast(int, instruction.p2)
                except StopIteration:
                    cursor += 1


            if instruction.opcode == Opcode.Close:
                del btrees[instruction.p1]
                cursor += 1

            if instruction.opcode == Opcode.Halt:
                if instruction.p1 != 0:
                    # We have an error
                    raise Exception(instruction.p4)
                break

        print("---end_statement---")
        return
