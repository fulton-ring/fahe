"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Download } from "lucide-react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { convertToCSV, downloadCSV } from "@/lib/downloadUtils";
import type { SQLResult } from "@/lib/parseSQLResults";

interface SQLResultTableProps {
  result: SQLResult;
}

export const SQLResultTable = ({ result }: SQLResultTableProps) => {
  if (!result.rows || result.rows.length === 0) {
    return <div className="text-xs text-gray-500 italic">No results found</div>;
  }

  const columns = Object.keys(result.rows[0]);
  const rowCount = result.rowCount;

  // Collapse by default if more than 10 rows
  const shouldCollapseByDefault = rowCount > 10;
  const [isOpen, setIsOpen] = useState(!shouldCollapseByDefault);

  const handleExportCSV = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    const csv = convertToCSV(result.rows);
    const timestamp = new Date()
      .toISOString()
      .replace(/[:.]/g, "-")
      .slice(0, -5);
    downloadCSV(csv, `query_results_${timestamp}.csv`);
  };

  const handleToggle = () => {
    setIsOpen(!isOpen);
  };

  const tableContent = (
    <div className="my-2 max-w-full overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            {columns.map((col) => (
              <TableHead key={col} className="text-xs font-semibold">
                {col}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {result.rows.map((row, idx) => (
            <TableRow key={idx}>
              {columns.map((col) => (
                <TableCell key={col} className="text-xs">
                  {row[col] !== null && row[col] !== undefined
                    ? String(row[col])
                    : "—"}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );

  return (
    <div className="my-2">
      {/* Header with row count and export button */}
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs text-gray-700 font-medium">
          📊 {rowCount} {rowCount === 1 ? "row" : "rows"}
        </span>
        <Button
          variant="outline"
          size="sm"
          onClick={handleExportCSV}
          className="h-7 px-3 text-xs shrink-0 hover:bg-primary hover:text-primary-foreground transition-colors"
          title="Export to CSV"
        >
          <Download className="h-3 w-3 mr-1.5" />
          CSV
        </Button>
      </div>

      {/* Collapsible table */}
      <details
        open={isOpen}
        className="cursor-pointer [&_summary]:list-none [&_summary::-webkit-details-marker]:hidden"
      >
        <summary
          onClick={(e) => {
            e.preventDefault();
            handleToggle();
          }}
          className="text-xs text-gray-600 hover:text-gray-900 mb-1 select-none flex items-center"
        >
          <span
            className={`inline-block transition-transform ${
              isOpen ? "rotate-90" : ""
            }`}
          >
            ▶
          </span>
          <span className="ml-1">
            {isOpen ? "Click to collapse table" : "Click to expand table"}
          </span>
        </summary>
        {tableContent}
      </details>
    </div>
  );
};
