// ==UserScript==
// @name         pullHTML
// @namespace    http://tampermonkey.net/
// @version      2024-04-27
// @description  try to take over the world!
// @author       You
// @match        https://dexscreener.com/new-pairs?rankBy=pairAge&order=asc&chainIds=solana&minLiq=1000
// @icon         https://www.google.com/s2/favicons?sz=64&domain=dexscreener.com
// @grant        none
// ==/UserScript==

(function() {
    'use strict';

    // Personal script is below
    function sendDataAfterDelay() {
        // Find the div with the specific class
        var targetDiv = document.querySelector('.ds-dex-table.ds-dex-table-new');
        if (targetDiv) {
            var htmlContent = targetDiv.outerHTML; // Get the outer HTML of the div

            // Use the Fetch API to send the data
            fetch('http://localhost:3000/receive', {
                method: 'POST',
                headers: {
                    'Content-Type': 'text/html'
                },
                body: htmlContent
            }).then(response => response.json())
                .then(data => console.log('Success:', data))
                .catch((error) => {
                console.error('Error making request:', error);
            });
        } else {
            console.log('Target div not found.');
        }
    }

    // Wait for 2 seconds before running the function for the first time
    setTimeout(() => {
        sendDataAfterDelay();
        // Schedule sendDataAfterDelay to run every minute (60000 milliseconds)
        setInterval(sendDataAfterDelay, 60000);
    }, 2000);



    // Personal script is above
})();
