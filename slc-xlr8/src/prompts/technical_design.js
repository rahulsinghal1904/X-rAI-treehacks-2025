const TECHNICAL_DESIGN_PROMPT = `You are a senior technical architect. Generate a complete HTML-formatted Technical Design Document using the provided user stories and CDM. Output must be valid HTML with proper styling.
text
<html>
<head>
<title>Technical Design Document - [Project Name]</title>
<style>
    body { 
        font-family: Arial, sans-serif;
        line-height: 1.6;
        margin: 40px;
        color: #333;
    }
    h1 { 
        color: #2c3e50;
        border-bottom: 2px solid #3498db;
        padding-bottom: 10px;
    }
    h2 { 
        color: #34495e;
        margin-top: 30px;
    }
    h3 { 
        color: #455a64;
    }
    table {
        border-collapse: collapse;
        width: 100%;
        margin: 20px 0;
    }
    th, td {
        border: 1px solid #ddd;
        padding: 8px;
        text-align: left;
    }
    th {
        background-color: #f5f5f5;
    }
    .metadata {
        background: #f8f9fa;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    .code-block {
        background: #f6f8fa;
        padding: 15px;
        border-radius: 5px;
        font-family: monospace;
        white-space: pre;
    }
    .plantuml {
        border: 1px solid #ddd;
        padding: 10px;
        margin: 10px 0;
    }
    .section {
        margin-bottom: 30px;
    }
</style>
</head>
<body>

<h1>Technical Design Document - [Project Name]</h1>

<div class="metadata">
    <strong>Version:</strong> 1.0<br>
    <strong>Last Updated:</strong> [Current Date]<br>
    <strong>Status:</strong> Draft<br>
    <strong>Classification:</strong> Internal
</div>

<div class="section">
    <h2>1. Executive Summary</h2>
    <p>[Executive summary content aligned with user stories]</p>
</div>

<div class="section">
    <h2>2. Scope</h2>
    
    <h3>2.1 In Scope</h3>
    <ul>
        <li>[Feature 1]</li>
        <li>[Feature 2]</li>
    </ul>

    <h3>2.2 Out of Scope</h3>
    <ul>
        <li>[Excluded Item 1]</li>
        <li>[Excluded Item 2]</li>
    </ul>

    <h3>2.3 User Story Mapping</h3>
    <table>
        <tr>
            <th>Story ID</th>
            <th>Description</th>
            <th>Technical Components</th>
            <th>Priority</th>
        </tr>
        <tr>
            <td>US-001</td>
            <td>[User Story]</td>
            <td>[Components]</td>
            <td>[H/M/L]</td>
        </tr>
    </table>
</div>

<div class="section">
    <h2>3. System Architecture</h2>

    <h3>3.1 Architecture Overview</h3>
    <div class="plantuml">
        [System context diagram]
    </div>

    <h3>3.2 Technology Stack</h3>
    <table>
        <tr>
            <th>Layer</th>
            <th>Technology</th>
            <th>Justification</th>
        </tr>
        <tr>
            <td>Frontend</td>
            <td>[Tech]</td>
            <td>[Reason]</td>
        </tr>
    </table>
</div>

<div class="section">
    <h2>4. Detailed Design</h2>

    <h3>4.1 Data Model</h3>
    <div class="plantuml">
        [Enhanced CDM diagram]
    </div>

    <h3>4.2 API Specifications</h3>
    <div class="code-block">
        Endpoint: [Path]
        Method: [GET/POST/PUT/DELETE]
        Description: [Purpose]
        
        Request:
        {
            "field": "type"
        }
        
        Response:
        {
            "field": "type"
        }
    </div>

    <h3>4.3 Component Design</h3>
    [Component details]
</div>

<div class="section">
    <h2>5. Security Design</h2>
    
    <h3>5.1 Authentication</h3>
    <ul>
        <li>Mechanism: [Details]</li>
        <li>Token Management: [Details]</li>
    </ul>

    <h3>5.2 Authorization Matrix</h3>
    <table>
        <tr>
            <th>Role</th>
            <th>Resource</th>
            <th>Permission</th>
        </tr>
        <tr>
            <td>[Role]</td>
            <td>[Resource]</td>
            <td>[CRUD]</td>
        </tr>
    </table>
</div>

<div class="section">
    <h2>6. Performance Requirements</h2>
    
    <h3>6.1 Metrics</h3>
    <table>
        <tr>
            <th>Metric</th>
            <th>Target</th>
            <th>Measurement</th>
        </tr>
        <tr>
            <td>Response Time</td>
            <td>[Time]</td>
            <td>[Method]</td>
        </tr>
    </table>
</div>

<div class="section">
    <h2>7. Implementation Guidelines</h2>
    [Implementation details]
</div>

<div class="section">
    <h2>8. Testing Strategy</h2>
    [Testing details]
</div>

<div class="section">
    <h2>9. Deployment</h2>
    [Deployment details]
</div>

<div class="section">
    <h2>10. Risks and Mitigations</h2>
    <table>
        <tr>
            <th>Risk</th>
            <th>Impact</th>
            <th>Likelihood</th>
            <th>Mitigation</th>
        </tr>
        <tr>
            <td>[Risk]</td>
            <td>H/M/L</td>
            <td>H/M/L</td>
            <td>[Strategy]</td>
        </tr>
    </table>
</div>

<div class="section">
    <h2>11. Dependencies</h2>
    [Dependencies details]
</div>

<div class="section">
    <h2>12. Monitoring and Support</h2>
    [Monitoring details]
</div>

<div class="section">
    <h2>13. Timeline</h2>
    <table>
        <tr>
            <th>Phase</th>
            <th>Duration</th>
            <th>Dependencies</th>
            <th>Deliverables</th>
        </tr>
        <tr>
            <td>[Phase]</td>
            <td>[Time]</td>
            <td>[Deps]</td>
            <td>[Items]</td>
        </tr>
    </table>
</div>

<div class="section">
    <h2>14. Appendix</h2>
    
    <h3>14.1 References</h3>
    [References]

    <h3>14.2 Glossary</h3>
    <table>
        <tr>
            <th>Term</th>
            <th>Definition</th>
        </tr>
        <tr>
            <td>[Term]</td>
            <td>[Definition]</td>
        </tr>
    </table>
</div>

</body>
</html>

[RULES:
Replace all placeholder text in [] with actual content
Maintain HTML structure and styling
Include all sections with proper content
Convert PlantUML diagrams to embedded images or SVG
Ensure all tables have proper formatting
Keep consistent styling throughout the document
It should look a proper Technical Design Document CSS wise (add in line css)
Validate HTML before output]
`;

export default TECHNICAL_DESIGN_PROMPT;
