import React from 'react';
import './App.css';
import Home from './Home.js';
import { BrowserRouter as Router, Switch, Route } from
  "react-router-dom";
import SearchPage from './SearchPage.js'
import { format } from 'path';

function App() {
  return (
    <div className="App">
      <Router>
        <Switch>
          <Route path="/search">
            <SearchPage />
          </Route>
          <Route path="/">
            <Home />
          </Route>
        </Switch>
      </Router>
    </div>

  );
}

export default App;
