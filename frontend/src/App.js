import React, { useEffect, useState } from "react";

function App() {
    const [items, setItems] = useState([]);
    const [status, setStatus] = useState("Loading...");

    useEffect(() => {
        // Backend is at http://localhost:8000 when exposed via compose
        fetch("http://localhost:8000/api/items")
            .then((res) => res.json())
            .then((data) => {
                setItems(data.items || []);
                setStatus("OK");
            })
            .catch((err) => {
                console.error(err);
                setStatus("Error");
            });
    }, []);

    return (
        <div style={{ fontFamily: "sans-serif", padding: 20 }}>
            <h1>Minimal React → FastAPI → Postgres</h1>
            <p>Status: {status}</p>
            <ul>
                {items.map((it) => (
                    <li key={it.id}>
                        {it.id}: {it.name}
                    </li>
                ))}
            </ul>
        </div>
    );
}

export default App;
