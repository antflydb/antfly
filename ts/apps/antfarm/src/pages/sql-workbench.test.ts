import { describe, expect, it } from "vitest";
import { sqlCell, sqlParameters } from "./sql-workbench";

describe("SQL workbench typed values", () => {
  it("preserves exact integers and distinguishes SQL NULL from JSON null", () => {
    const result = {
      columns: [],
      rows: [["9007199254740993", null, null]],
      sql_nulls: [[false, true, false]],
      rows_affected: 0,
      command_tag: "SELECT",
    };
    expect(sqlCell(result, 0, 0)).toBe("9007199254740993");
    expect(sqlCell(result, 0, 1)).toBe("NULL");
    expect(sqlCell(result, 0, 2)).toBe("null");
  });
  it("rejects lossy parameters before submission", () => {
    expect(() => sqlParameters("[9007199254740993]")).toThrow("quoted decimal");
    expect(sqlParameters('["9007199254740993",null]')).toEqual(["9007199254740993", null]);
    expect(() => sqlParameters("{}")).toThrow("JSON array");
  });
});
