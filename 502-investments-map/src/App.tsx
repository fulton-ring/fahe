import { CopilotKit } from "@copilotkit/react-core";
import Map from "./components/Map";
import "./App.css";

function App() {
  return (
    <CopilotKit
      publicApiKey="ck_pub_52b848ba3425601532efbce0e1dcb37a"
      showDevConsole={true}
    >
      <div style={{ width: "100vw", height: "100vh", overflow: "hidden" }}>
        <Map />
      </div>
    </CopilotKit>
  );
}

export default App;
