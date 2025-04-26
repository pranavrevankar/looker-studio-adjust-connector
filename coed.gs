/**
 * This is the script of Google Data studio community 
 * connector for Adjust.
 * This fethces the data from Adjust and allows the user
 * to visualize the data in Google Data studio
 */

/**
 * The AuthType of this connector is USER_TOKEN
 */


function getAuthType() {
  var cc = DataStudioApp.createCommunityConnector();
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.USER_TOKEN)
    .setHelpUrl('https://www.example.org/connector-auth-help')
    .build();
}

/**
 * Resets the auth service.
 */
function resetAuth() {
  var user_tokenProperties = PropertiesService.getUserProperties();
  user_tokenProperties.deleteProperty('dscc.username');
  user_tokenProperties.deleteProperty('dscc.password');
}

/**
 * Returns true if the auth service has access.
 * @return {boolean} True if the auth service has access.
 */
function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var userName = userProperties.getProperty('dscc.username');
  var token = userProperties.getProperty('dscc.token');
  return true;
}


/**
 * Sets the credentials.
 * @param {Request} request The set credentials request.
 * @return {object} An object with an errorCode.
 */
function setCredentials(request) {
  var creds = request.userToken;
  var username = creds.username;
  var token = creds.token;
  var userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty('dscc.username', username);
  userProperties.setProperty('dscc.token', token);
  return {
    errorCode: 'NONE'
  };
}


/**
 * To connect Adjust account, we need two information
 * The App_key which is specific to each App
 * And the user_token that is required to make an API call.
 * Requesting these two information in the getConfig
 */

function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();
  
  config.newInfo()
    .setId('instructions')
    .setText('Enter the Adjust APP token and the User token for authentication.')
  
  config.newTextInput()
    .setId('app_key')
    .setName('Enter the App token')
    .setHelpText('You can find the App token in the url of the dashboard when you enter the App dashboard(between "default/" and "?").')
    .setPlaceholder('69urg76291d');
  
  config.newTextInput()
    .setId('user_token')
    .setName('Enter the User token')
    .setHelpText('You can find this in the account settings of Adjust.')
    .setPlaceholder('7ChymaNx4YZeAepVTbh5');
   
  config.setDateRangeRequired(true);
  
  return config.build();
}


/**
 * Adjust has a set of standard metrics like Installs, clicks etc.
 * Further there are some metrics that are specific to each app like add_to_cart
 * These app specific metrics are called as "events"
 * Although more dimensions are available to query in Adjust
 * This script is only querying Network, Country and Date
 * to avoid data bloat.
 */

function getFields(request, events) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  
  var std_metrics = ['Installs', 'Revenue', 'Clicks', 'Uninstalls', 'Impressions','Uninstall_cohort', 
                     'Reinstalls', 'Reattributions', 'Deattributions', 'Sessions', 'DAUS', 'MAUS', 'WAUS', 
                     'install_cost', 'click_cost','impression_cost','cost','paid_installs'];
  
  var metrics = std_metrics.concat(events);
  
  metrics.forEach(function (value){
  fields.newMetric()
    .setId(value)
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    console.log("getFields.value=>", value);
  });
  
  fields.newDimension()
    .setId('Network')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('Date')
    .setType(types.YEAR_MONTH_DAY);
  
  fields.newDimension()
    .setId('Country')
    .setType(types.TEXT);
 
  return fields;
}

/** 
 * To get the event metrics which are specific to the app,
 * one API call is done to retrieve the list of all events.
 * this is then passed to the getFields function.
 */
  

function getSchema(request) {
  var url = 
    'https://api.adjust.com/kpis/v1/'+
     request.configParams.app_key +
    '?user_token='+
    request.configParams.user_token +
    '&grouping=app' +
    '&human_readable_kpis=true' +
    '&event_kpis=all_events';
  var response = UrlFetchApp.fetch(url);
  console.log(response);
  var parsedResponseEvent = JSON.parse(response).result_parameters.events;
  console.log(parsedResponseEvent);
  var events = [];
  events = parsedResponseEvent.map(function(oneEvent){
    return oneEvent.name;
  });
  console.log(events);
  
  var fields = getFields(request, events).build();
  return { schema: fields };
}

/**
 * The data from the API which is in the form of JSON
 * is being transformed to the required format. 
 */

function responseToRows(requestedFields, response, events) {
  var std_metrics = ['Installs','Revenue', 'Clicks', 'Uninstalls', 'Impressions',
                     'Uninstall_cohort', 'Reinstalls', 'Reattributions', 'Deattributions', 
                     'Sessions', 'DAUS', 'MAUS', 'WAUS', 'install_cost', 'click_cost',
                     'impression_cost','cost','paid_installs'];
  var metrics = std_metrics.concat(events);
  var final_array = [];
  // Transform parsed data and filter for requested fields
  final_array = response.map(function(oneNetwork) {
    var temp_array = [];
    var networkName = oneNetwork.name;
    
      oneNetwork.countries.forEach(function (oneCountry) {
      var countryName = oneCountry.country;
      oneCountry.dates.forEach(function (dateDetails) {
      var row = [];
        requestedFields.asArray().forEach(function (field) {
          switch (field.getId()) {
            case 'Date':
              return row.push(dateDetails.date.replace(/-/g, ''));
            case 'Network':
              return row.push(networkName);
            case 'Country':
              return row.push(countryName);
            default:
              return row.push(dateDetails.kpi_values[metrics.indexOf(field.getId())]);
          }
        });
      temp_array.push({values: row });
    });
      });
    
    return temp_array;
  });
  var final_response =  [].concat.apply([], final_array);
  return final_response;
}

/**
 * The getData function contains the API query to the Adjust API. 
 * Its currently using a fixed query URL but this 
 * can be potentially changed to a dynamic URL based on the
 * requested fields.
 */

function getData(request) {
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  }); 
  
  var url = 
    'https://api.adjust.com/kpis/v1/'+
     request.configParams.app_key +
    '?user_token='+
    request.configParams.user_token +
    '&grouping=network, country, day' + 
    '&kpis=installs, revenue, clicks, uninstalls, impressions, uninstall_cohort, reinstalls, reattributions, deattributions, sessions, daus, maus, waus, install_cost, click_cost,impression_cost,cost,paid_installs' + 
    '&human_readable_kpis=true' +
    '&event_kpis=all_events' +
    '&start_date='+ request.dateRange.startDate +
    '&end_date='+ request.dateRange.endDate; 
  var response = UrlFetchApp.fetch(url);
  
  var parsedResponseEvents = JSON.parse(response).result_parameters.events;
  var events = [];
  events = parsedResponseEvents.map(function(oneEvent){
    return oneEvent.name;
  });
  
  var requestedFields = getFields(request, events).forIds(requestedFieldIds);
  var parsedResponse = JSON.parse(response).result_set.networks;
  var rows = responseToRows(requestedFields, parsedResponse, events);
  return {
    schema: requestedFields.build(),
    rows: rows
  };
}
