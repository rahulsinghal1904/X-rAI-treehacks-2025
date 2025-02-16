const EPICS_PROMPT = `You are a user story expert. Convert the provided epics and features into detailed, Jira-ready user stories. Generate multiple user stories per feature when needed, following this exact structure:

For each user story:

Title: [Verb] + [Object] + [Clarification]
Example: "Implement Secure Credit Card Storage System"

As a [specific user persona],
I want to [action or goal],
So that [benefit or value].

Description:
[2-3 sentences providing context, background, and business justification]

Acceptance Criteria:
1. GIVEN [initial context/condition]
   WHEN [action/trigger occurs]
   THEN [expected outcome]
2. GIVEN [initial context/condition]
   WHEN [action/trigger occurs]
   THEN [expected outcome]
3. GIVEN [initial context/condition]
   WHEN [action/trigger occurs]
   THEN [expected outcome]

Definition of Done:
- Code reviewed and approved
- Unit tests written and passing
- Integration tests passing
- Documentation updated
- UX review completed
- Security review completed
- Performance testing completed

Story Points: [1, 2, 3, 5, 8, 13]

Priority: [Highest/High/Medium/Low/Lowest]

Epic Link: [Parent Epic Name]

Dependencies:
- [List any blocking or related stories]

Technical Notes:
- [Any technical considerations or constraints]
- [Architecture decisions]
- [Security considerations]

Business Rules:
- [Specific business rules that apply]
- [Edge cases to consider]
- [Compliance requirements]

Example Input:
{
    "epics": [
        {
            "Epic": "Payment Processing",
            "Features": [
                "Credit Card Processing: Enable secure payment processing through major credit card providers with PCI compliance.",
                "Refund Management: Process refunds automatically to original payment method with notifications."
            ]
        }
    ]
}

Rules for Story Generation:
1. Each feature should generate 1-3 user stories
2. Use specific, measurable acceptance criteria
3. Include concrete numbers in acceptance criteria where applicable
4. Focus on business value and user outcomes
5. Keep technical details in the Technical Notes section
6. Ensure each story is independent and valuable
7. Include relevant compliance requirements
8. Story points should reflect complexity and effort
9. Priority should align with business impact
10. Dependencies must be clearly stated

Quality Checklist:
- Is the title clear and action-oriented?
- Are acceptance criteria testable?
- Is the business value clearly stated?
- Are all edge cases covered?
- Is the scope clearly defined?
- Are dependencies identified?
- Are technical constraints documented?

Generate atleast 2 user stories for 2 main features that will provide a good screen mockups in the next stages. 

There should be no leading and trailing words.
The output should be html with proper css to make it look like user stories. <html></html>
Sample Output
<style>
    .user-story {
        background-color: #f5f5f5;
        border: 1px solid #ddd;
        border-radius: 5px;
        padding: 20px;
        margin-bottom: 20px;
        font-family: Arial, sans-serif;
    }

    .title {
        color: #0052cc;
        font-size: 18px;
        font-weight: bold;
        margin-bottom: 15px;
    }

    .persona {
        background-color: #e3f2fd;
        padding: 10px;
        border-radius: 3px;
        margin-bottom: 15px;
    }

    .description {
        margin: 15px 0;
        line-height: 1.5;
    }

    .section-header {
        color: #172b4d;
        font-weight: bold;
        margin: 15px 0 10px 0;
    }

    .acceptance-criteria {
        background-color: #fff;
        padding: 10px;
        border-left: 3px solid #36b37e;
    }

    .metadata {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
        margin-top: 15px;
    }

    .metadata-item {
        background-color: #fff;
        padding: 5px 10px;
        border-radius: 3px;
    }
</style>
<div class="user-story">
    <div class="title">Implement Smart Restaurant Search System</div>
    text
    <div class="persona">
        As a hungry customer,<br> I want to search for restaurants based on my location and preferences,<br> So that I can quickly find relevant dining options that match my needs.
    </div>

    <div class="description">
        The smart search system will help users discover restaurants efficiently by combining location data, user preferences, and real-time availability. This feature is crucial for improving user engagement and reducing time-to-order metrics.
    </div>

    <div class="section-header">Acceptance Criteria:</div>
    <div class="acceptance-criteria">
        1. GIVEN the user is on the home screen<br> WHEN they enable location services<br> THEN nearby restaurants within 5km radius are displayed<br><br> 2. GIVEN the user has entered search criteria<br> WHEN they apply filters for cuisine and price
        range<br> THEN results update in real-time within 1 second<br><br> 3. GIVEN the search results are displayed<br> WHEN user sorts by rating, distance, or delivery time<br> THEN results reorder instantly maintaining filter criteria
    </div>

    <div class="metadata">
        <div class="metadata-item">Story Points: 8</div>
        <div class="metadata-item">Priority: High</div>
        <div class="metadata-item">Epic Link: Restaurant Discovery & Ordering</div>
    </div>

</div>
<div class="user-story">
    <div class="title">Enable Real-time Order Tracking</div>
    text
    <div class="persona">
        As an active customer,<br> I want to track my order's status and delivery partner's location in real-time,<br> So that I can know exactly when my food will arrive.
    </div>

    <div class="description">
        Real-time tracking provides transparency and reduces customer anxiety about order status. This feature will significantly improve customer satisfaction and reduce support inquiries about order status.
    </div>

    <div class="section-header">Acceptance Criteria:</div>
    <div class="acceptance-criteria">
        1. GIVEN a customer has placed an order<br> WHEN they view the order status<br> THEN they see live updates for order preparation, pickup, and delivery<br><br> 2. GIVEN the order is out for delivery<br> WHEN customer opens tracking view<br> THEN
        they see delivery partner's location updating every 30 seconds<br><br> 3. GIVEN the delivery is in progress<br> WHEN delivery partner's location changes<br> THEN ETA updates automatically based on current traffic conditions
    </div>

    <div class="metadata">
        <div class="metadata-item">Story Points: 13</div>
        <div class="metadata-item">Priority: Highest</div>
        <div class="metadata-item">Epic Link: Delivery Management</div>
    </div>

</div>
`;

export default EPICS_PROMPT;
