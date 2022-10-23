from toysql.compiler import Program, Opcode
from toysql.record import DataType, Record
from toysql.btree import BTree, Cursor
from typing import cast
from toysql.exceptions import NotFoundException


class VM:
    def __init__(self, pager):
        self.pager = pager

    def execute(self, program: Program):
        btrees = {}
        registers = {}
        cursor = 0

        while True:
            instruction = program.instructions[cursor]
            print(instruction.opcode)

            if instruction.opcode == Opcode.Init:
                # init instruction tells us which address to start at.
                cursor = cast(int, instruction.p2)

            if instruction.opcode == Opcode.Transaction:
                # No support for Transactions yet.
                cursor += 1

            if instruction.opcode == Opcode.CreateBtree:
                # TODO: Should be able to roll this back.
                # RN: pager.new() will write to disk.
                page_number = self.pager.new()
                registers[instruction.p2] = page_number
                cursor += 1

            if instruction.opcode == Opcode.Goto:
                # Unconditional jump to instruction at address p2
                cursor = cast(int, instruction.p2)

            if instruction.opcode == Opcode.SCopy:
                # shallow copy register value p1 -> p2.
                registers[instruction.p2] = registers[instruction.p1]
                cursor += 1

            if instruction.opcode == Opcode.OpenWrite:
                # Open btree with write cursor (Currently cursors don't have read/write flag)
                # TODO: Also p4 is unimplemeneted.
                btrees[instruction.p1] = Cursor(BTree(self.pager, instruction.p2))
                cursor += 1

            if instruction.opcode == Opcode.OpenRead:
                # Open a cursor with root page p2 and assign its refname to val p1
                btrees[instruction.p1] = Cursor(BTree(self.pager, instruction.p2))
                cursor += 1

            if instruction.opcode == Opcode.SoftNull:
                # Not Implemented.
                cursor += 1

            if instruction.opcode == Opcode.NotNull:
                if registers[instruction.p1] is not None:
                    cursor = cast(int, instruction.p2)
                else:
                    cursor += 1

            if instruction.opcode == Opcode.IsNull:
                # If p1 addr is null jump to p2
                if registers[instruction.p1] is None:
                    cursor = cast(int, instruction.p2)
                else:
                    cursor += 1

            if instruction.opcode == Opcode.String:
                registers[instruction.p2] = instruction.p4
                cursor += 1

            if instruction.opcode == Opcode.Integer:
                registers[instruction.p2] = instruction.p1
                cursor += 1

            if instruction.opcode == Opcode.NewRowid:
                registers[instruction.p2] = btrees[instruction.p1].new_row_id()
                cursor += 1

            if instruction.opcode == Opcode.SeekRowid:
                # TODO propery implement Seek in Btree module.
                tree = btrees[instruction.p1]
                try:
                    tree.seek(registers[instruction.p3])
                    cursor += 1
                except NotFoundException:
                    cursor = cast(int, instruction.p2)

            if instruction.opcode == Opcode.MustBeInt:
                # Force the value in register P1 to be an integer.
                # If the value in P1 is not an integer and cannot be converted into an integer without data loss
                # then jump immediately to P2, or if P2==0 raise an SQLITE_MISMATCH exception.
                try:
                    registers[instruction.p1] = int(registers[instruction.p1])
                    cursor += 1
                except ValueError:
                    if instruction.p2 == 0:
                        # TODO create a custom error type for TypeErrors
                        raise ValueError("TOYSQL TYPE MISMATCH")
                    cursor = cast(int, instruction.p2)

            if instruction.opcode == Opcode.Noop:
                cursor += 1

            if instruction.opcode == Opcode.NotExists:
                record = btrees[instruction.p1].find(registers[instruction.p3])

                # if record:
                #     raise Exception("It found")

                if record is None:
                    cursor = cast(int, instruction.p2)
                else:
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

            if instruction.opcode == Opcode.Rowid:
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
                assert instruction.p4
                values = []

                for i in range(instruction.p2):
                    v = []
                    type_affinity = instruction.p4[i]

                    # Add column datatype (from affinity)
                    v.append(DataType.from_affinity(type_affinity))

                    # Add column value
                    v.append(registers[instruction.p1 + i])
                    values.append(v)

                record = Record(values)

                # Not sure if this is right.
                registers[instruction.p3] = record
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

            if instruction.opcode == Opcode.Insert:
                record = registers[instruction.p2]
                btrees[instruction.p1].insert(record)
                registers[instruction.p3] = record.row_id
                registers[instruction.p2] = record
                cursor += 1

            if instruction.opcode == Opcode.Next:
                # Check if btree cursor p1 has next value.
                # If next continue to address p2
                # else fall through to next instruction.
                tree = btrees[instruction.p1]

                try:
                    v = next(tree)
                    cursor = cast(int, instruction.p2)
                except StopIteration:
                    cursor += 1

            if instruction.opcode == Opcode.Halt:
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
