using Nbn.Runtime.SettingsMonitor;

var options = SettingsMonitorOptions.FromArgs(args);
var store = new SettingsMonitorStore(options.DatabasePath);
await store.InitializeAsync();

Console.WriteLine($"SettingsMonitor storage ready at {Path.GetFullPath(options.DatabasePath)}");
