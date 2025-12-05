import React, { useEffect, useState } from "react";
import {
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
} from "@mui/material";
import { PieChart } from "@mui/x-charts/PieChart";

export default function SeatDistribution() {
  const [data, setData] = useState([]);
  const [status, setStatus] = useState("Loading...");

  useEffect(() => {
    fetch("http://localhost:8000/national_summary_25")
      .then((res) => res.json())
      .then((json) => {
        // Convert backend rows (tuples) into objects for PieChart + table
        const formatted = (json.data || []).map(([partei, seats], idx) => ({
          id: idx,
          value: seats,
          label: partei,
        }));
        setData(formatted);
        setStatus("OK");
      })
      .catch(() => setStatus("Error"));
  }, []);

  return (
    <div style={{ padding: 20 }}>
      <Typography variant="h4" gutterBottom>
        Sitzverteilung im Bundestag 2025
      </Typography>

      {/* Pie chart with MUI X */}
      <PieChart
        series={[
          {
            data: data,
            highlightScope: { faded: "global", highlighted: "item" },
            faded: { innerRadius: 30, additionalRadius: -30, color: "gray" },
          },
        ]}
        width={500}
        height={400}
      />

      {/* Table */}
      <TableContainer component={Paper} style={{ marginTop: 20 }}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Partei</TableCell>
              <TableCell align="right">Anzahl Sitze</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.map((row) => (
              <TableRow key={row.id}>
                <TableCell>{row.label}</TableCell>
                <TableCell align="right">{row.value}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
}
