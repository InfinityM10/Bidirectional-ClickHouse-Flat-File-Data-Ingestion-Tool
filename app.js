document.addEventListener('DOMContentLoaded', function() {
    // Source selection handling
    const sourceTypeRadios = document.querySelectorAll('input[name="sourceType"]');
    const clickhouseSourceConfig = document.getElementById('clickhouseSourceConfig');
    const flatfileSourceConfig = document.getElementById('flatfileSourceConfig');
    
    // State management
    const appState = {
        sourceType: 'clickhouse',
        clickhouseConfig: {
            host: '',
            port: '',
            database: '',
            username: '',
            jwtToken: ''
        },