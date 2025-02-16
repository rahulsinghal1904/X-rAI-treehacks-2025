// Loader.js
import React from 'react';
import './Loader.css';

function Loader() {
  return (
    <div className="loader-overlay">
      <div className="loader">
        <div className="spinner"></div>
      </div>
    </div>
  );
}

export default Loader;
