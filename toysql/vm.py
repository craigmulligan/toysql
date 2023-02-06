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
        print(program)

        while len(program.instructions) < cursor:
            instruction = program.instructions[cursor]
            if instruction.opcode == Opcode.CreateTable:
                # old
                # TODO: Should be able to roll this back.
                # RN: pager.new() will write to disk.
                page_number = self.pager.new()
                registers[instruction.p2] = page_number
                cursor += 1

            if instruction.opcode == Opcode.SCopy:
                # old
                # shallow copy register value p1 -> p2.
                registers[instruction.p2] = registers[instruction.p1]
                cursor += 1

            if instruction.opcode == Opcode.OpenWrite:
                # old
                # Open btree with write cursor (Currently cursors don't have read/write flag)
                # TODO: Also p4 is unimplemeneted.
                btrees[instruction.p1] = Cursor(BTree(self.pager, instruction.p2))
                cursor += 1

            if instruction.opcode == Opcode.OpenRead:
                # old
                # Open a cursor with root page p2 and assign its refname to val p1
                btrees[instruction.p1] = Cursor(BTree(self.pager, instruction.p2))
                cursor += 1

            if instruction.opcode == Opcode.String:
                # old
                registers[instruction.p2] = instruction.p4
                cursor += 1

            if instruction.opcode == Opcode.Integer:
                # old
                registers[instruction.p2] = instruction.p1
                cursor += 1

            if instruction.opcode == Opcode.Noop:
                cursor += 1

            if instruction.opcode == Opcode.Rewind:
                # old
                # If table or index is empty jump to p2
                # else rewind the btree cursor to start.
                tree = btrees[instruction.p1]

                if tree.row_count() == 0:
                    cursor = cast(int, instruction.p2)
                else:
                    btrees[instruction.p1].reset()
                    cursor += 1

            if instruction.opcode == Opcode.Key:
                # old
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
                    v = registers[instruction.p1 - 1 + i]

                    t = DataType.integer if isinstance(v, int) else DataType.text  
                    c.append(t)
                    c.append(registers[instruction.p1 - 1 + i])
                    values.append(c)

                record = Record(values)

                # Not sure if this is right.
                registers[instruction.p3] = record
                cursor += 1

            if instruction.opcode == Opcode.ResultRow:
                # old
                # Take all the stored values in registers p1 - p2 and yeild them
                # to the caller.
                values = []
                for i in range(
                    cast(int, instruction.p1), cast(int, instruction.p2) + 1
                ):
                    values.append(registers[i])

                cursor += 1

                yield values

            if instruction.opcode == Opcode.Insert:
                record = registers[instruction.p2]
                btrees[instruction.p1].insert(record)
                registers[instruction.p3] = record.row_id
                registers[instruction.p2] = record
                cursor += 1

            if instruction.opcode == Opcode.Next:
                # old
                # Check if btree cursor p1 has next value.
                # If next continue to address p2
                # else fall through to next instruction.
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
                # old
                # Immediate exit.
                # P1 is the result code returned by sqlite3_exec(),
                # sqlite3_reset(), or sqlite3_finalize().
                # For a normal halt, this should be SQLITE_OK (0).
                # For errors, it can be some other value.
                # If P1!=0 then P2 will determine whether or not to rollback the current transaction.
                # Do not rollback if P2==OE_Fail. Do the rollback if P2==OE_Rollback.
                # If P2==OE_Abort, then back out all changes that have occurred during this execution of the VDBE,
                # but do not rollback the transaction.
                if instruction.p1 != 0:
                    # We have an error
                    raise Exception(instruction.p4)
                break

        print("---end_statement---")
        return
