import SeatDistribution from "./components/SeatDistribution";

function App() {
    return (
        <div style={{ fontFamily: "sans-serif", padding: 20 }}>
            <h1>Election Statistics</h1>

            {/* Each statistic is its own component */}
            <SeatDistribution />
        </div>
    );
}

export default App;
