package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/eclipse-symphony/symphony/cli/utils"
	"github.com/ghodss/yaml"
	"github.com/jedib0t/go-pretty/v6/table"
	nbi "github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
	margoCli "github.com/margo/dev-repo/poc/wfm/cli"
	"github.com/spf13/cobra"
)

var (
	// margo compliant server details
	margoServerHost   string
	margoServerPort   uint16
	northboundBaseURL = "v1alpha2/margo/nbi/v1"
)

var (
	// File related variables
	applyFromFile string
)

var MargoCmd = &cobra.Command{
	Use:   "wfm",
	Short: "Commands to showcase and enable Margo PoC on wfm (app package, deployment, devices)",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("\n%sMargo CLI - Use subcommands: apply, delete, list, get%s\n\n", utils.ColorBlue(), utils.ColorReset())
		cmd.Help()
	},
}

// Apply command
var MargoApplyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply a Margo application configuration from a YAML file",
	Run: func(cmd *cobra.Command, args []string) {
		if applyFromFile == "" {
			fmt.Printf("\n%sError: --file must be specified%s\n\n", utils.ColorRed(), utils.ColorReset())
			return
		}

		if err := applyAppConfig(applyFromFile); err != nil {
			fmt.Printf("\n%sApply failed: %s%s\n\n", utils.ColorRed(), err.Error(), utils.ColorReset())
			return
		}

		fmt.Printf("\n%sApplication configuration applied successfully%s\n\n", utils.ColorGreen(), utils.ColorReset())
	},
}

// Delete command
var MargoDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete application Pkgs",
}

var MargoDeleteAppPkgCmd = &cobra.Command{
	Use:   "app-pkg <app-pkg-id>",
	Short: "Delete a margo application Pkg",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		appPkgID := args[0]
		fmt.Println("appPkgIdto be deleted", appPkgID)

		if err := deleteAppPkg(appPkgID); err != nil {
			fmt.Printf("\n%sDelete failed: %s%s\n\n", utils.ColorRed(), err.Error(), utils.ColorReset())
			return
		}

		fmt.Printf("\n%sApplication Pkg %s deleted successfully%s\n\n", utils.ColorGreen(), appPkgID, utils.ColorReset())
	},
}

var MargoDeleteDeploymentCmd = &cobra.Command{
	Use:   "deployment <deployment-id>",
	Short: "Delete a margo application deployment",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		deploymentID := args[0]
		fmt.Println("deploymentId to be deleted", deploymentID)

		if err := deleteDeployment(deploymentID); err != nil {
			fmt.Printf("\n%sDelete failed: %s%s\n\n", utils.ColorRed(), err.Error(), utils.ColorReset())
			return
		}

		fmt.Printf("\n%sApplication Deployment %s deleted successfully%s\n\n", utils.ColorGreen(), deploymentID, utils.ColorReset())
	},
}

// List command
var MargoListCmd = &cobra.Command{
	Use:   "list",
	Short: "List application Pkgs",
}

var MargoListDevicesCmd = &cobra.Command{
	Use:   "devices",
	Short: "List all margo devices",
	Run: func(cmd *cobra.Command, args []string) {
		if err := listDevices(); err != nil {
			fmt.Printf("\n%sList failed: %s%s\n\n", utils.ColorRed(), err.Error(), utils.ColorReset())
			return
		}
	},
}

var MargoListAppPkgCmd = &cobra.Command{
	Use:   "app-pkg",
	Short: "List all margo application Pkgs",
	Run: func(cmd *cobra.Command, args []string) {
		if err := listAppPkgs(); err != nil {
			fmt.Printf("\n%sList failed: %s%s\n\n", utils.ColorRed(), err.Error(), utils.ColorReset())
			return
		}
	},
}

var MargoListDeploymentCmd = &cobra.Command{
	Use:   "deployment",
	Short: "List all margo application deployments",
	Run: func(cmd *cobra.Command, args []string) {
		if err := listDeployments(); err != nil {
			fmt.Printf("\n%sList failed: %s%s\n\n", utils.ColorRed(), err.Error(), utils.ColorReset())
			return
		}
	},
}

// Get command
var MargoGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get application Pkg details",
}

var MargoGetAppPkgCmd = &cobra.Command{
	Use:   "app-pkg <app-pkg-id>",
	Short: "Get details of a margo application Pkg",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		appPkgID := args[0]

		if err := getAppPkg(appPkgID); err != nil {
			fmt.Printf("\n%sGet failed: %s%s\n\n", utils.ColorRed(), err.Error(), utils.ColorReset())
			return
		}
	},
}

var MargoGetDeploymentCmd = &cobra.Command{
	Use:   "deployment <deployment-id>",
	Short: "Get details of a margo application deployment",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		deploymentID := args[0]

		if err := getDeployment(deploymentID); err != nil {
			fmt.Printf("\n%sGet failed: %s%s\n\n", utils.ColorRed(), err.Error(), utils.ColorReset())
			return
		}
	},
}

// Implementation functions
func applyAppConfig(filename string) error {
	// Read the YAML file
	yamlFile, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Unmarshal the YAML into a generic map
	var data map[string]interface{}
	err = yaml.Unmarshal(yamlFile, &data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	// Determine the type of resource and call the appropriate function
	kind, ok := data["kind"].(string)
	if !ok {
		return fmt.Errorf("kind not found or not a string")
	}

	jsonFile, err := convertYamlToJson(yamlFile)
	if err != nil {
		return fmt.Errorf("failed to convert yaml to json: %w", err)
	}

	switch kind {
	case "ApplicationPackage":
		var appPkg nbi.ApplicationPackageManifestRequest
		err = json.Unmarshal(jsonFile, &appPkg)
		if err != nil {
			return fmt.Errorf("failed to unmarshal ApplicationPackage: %w", err)
		}
		return onboardAppPkg(&appPkg)
	case "ApplicationDeployment":
		var deployment nbi.ApplicationDeploymentManifestRequest
		err = json.Unmarshal(jsonFile, &deployment)
		if err != nil {
			return fmt.Errorf("failed to unmarshal ApplicationDeployment: %w", err)
		}
		return createDeployment(&deployment)
	default:
		return fmt.Errorf("unsupported kind: %s", kind)
	}
}

func onboardAppPkg(appPkg *nbi.ApplicationPackageManifestRequest) error {
	northboundCli := margoCli.NewWFMCli(margoServerHost, margoServerPort, &northboundBaseURL, nil)

	if appPkg == nil {
		return fmt.Errorf("no application Pkg specified")
	}

	resp, err := northboundCli.OnboardAppPkg(*appPkg)
	if err != nil {
		return fmt.Errorf("failed to upload application Pkg: %w", err)
	}

	if resp == nil {
		fmt.Println("response is nil")
		return nil
	}

	// fmt.Println("response", pretty.Sprint(*resp))
	// fmt.Println("pkgId", *resp.Metadata.Id, "pkgName", resp.Metadata.Name, "pkgVersion", resp.Spec)
	return nil
}

func createDeployment(deployment *nbi.ApplicationDeploymentManifestRequest) error {
	northboundCli := margoCli.NewWFMCli(margoServerHost, margoServerPort, &northboundBaseURL, nil)

	if deployment == nil {
		return fmt.Errorf("no application deployment specified")
	}

	resp, err := northboundCli.CreateDeployment(*deployment)
	if err != nil {
		return fmt.Errorf("failed to create application deployment: %w", err)
	}

	if resp == nil {
		fmt.Println("response is nil")
		return nil
	}

	fmt.Println("deploymentId", *resp.Metadata.Id, "deploymentName", resp.Metadata.Name)
	return nil
}

func deleteAppPkg(appPkgID string) error {
	northboundCli := margoCli.NewWFMCli(margoServerHost, margoServerPort, &northboundBaseURL, nil)

	err := northboundCli.DeleteAppPkg(appPkgID)
	if err != nil {
		return fmt.Errorf("failed to delete application Pkg: %w", err)
	}

	fmt.Println("app Pkg deletion request has been accepted!")
	return nil
}

func deleteDeployment(deploymentID string) error {
	northboundCli := margoCli.NewWFMCli(margoServerHost, margoServerPort, &northboundBaseURL, nil)

	err := northboundCli.DeleteDeployment(deploymentID)
	if err != nil {
		return fmt.Errorf("failed to delete application deployment: %w", err)
	}

	fmt.Println("application deployment deletion request has been accepted!")
	return nil
}

func listDevices() error {
	northboundCli := margoCli.NewWFMCli(margoServerHost, margoServerPort, &northboundBaseURL, nil)

	devices, err := northboundCli.ListDevices()
	if err != nil {
		return fmt.Errorf("failed to list application Pkgs: %w", err)
	}

	displayDevicesTable(*devices)
	return nil
}

func listAppPkgs() error {
	northboundCli := margoCli.NewWFMCli(margoServerHost, margoServerPort, &northboundBaseURL, nil)

	appPkgs, err := northboundCli.ListAppPkgs(margoCli.ListAppPkgsParams{})
	if err != nil {
		return fmt.Errorf("failed to list application Pkgs: %w", err)
	}

	displayAppPackagesTable(*appPkgs)
	return nil
}

func listDeployments() error {
	northboundCli := margoCli.NewWFMCli(margoServerHost, margoServerPort, &northboundBaseURL, nil)

	deployments, err := northboundCli.ListDeployments(margoCli.DeploymentListParams{})
	if err != nil {
		return fmt.Errorf("failed to list application deployments: %w", err)
	}

	displayDeploymentsTable(*deployments)
	return nil
}

func getAppPkg(appPkgID string) error {
	northboundCli := margoCli.NewWFMCli(margoServerHost, margoServerPort, &northboundBaseURL, nil)

	appPkg, err := northboundCli.GetAppPkg(appPkgID)
	if err != nil {
		return fmt.Errorf("failed to get application Pkg: %w", err)
	}

	// Display the details
	fmt.Printf("\n%sApplication Pkg Details:%s\n", utils.ColorBlue(), utils.ColorReset())
	printAppPkgDetails(appPkg)
	fmt.Println()
	return nil
}

func getDeployment(deploymentID string) error {
	northboundCli := margoCli.NewWFMCli(margoServerHost, margoServerPort, &northboundBaseURL, nil)

	deployment, err := northboundCli.GetDeployment(deploymentID)
	if err != nil {
		return fmt.Errorf("failed to get application deployment: %w", err)
	}

	// Display the details
	fmt.Printf("\n%sApplication Deployment Details:%s\n", utils.ColorBlue(), utils.ColorReset())
	printDeploymentDetails(deployment)
	fmt.Println()
	return nil
}

func init() {
	// Apply command flags
	MargoApplyCmd.Flags().StringVarP(&applyFromFile, "file", "f", "", "Path to the application configuration YAML file")

	// Global flags that apply to all Margo commands
	MargoCmd.PersistentFlags().StringVar(&margoServerHost, "host", "localhost", "Margo compliant WFM API server host")
	MargoCmd.PersistentFlags().Uint16Var(&margoServerPort, "port", 8082, "Margo compliant WFM API server port")

	// Build command hierarchy
	MargoCmd.AddCommand(MargoApplyCmd)

	MargoDeleteCmd.AddCommand(MargoDeleteAppPkgCmd)
	MargoDeleteCmd.AddCommand(MargoDeleteDeploymentCmd)

	MargoListCmd.AddCommand(MargoListAppPkgCmd)
	MargoListCmd.AddCommand(MargoListDeploymentCmd)
	MargoListCmd.AddCommand(MargoListDevicesCmd)

	MargoGetCmd.AddCommand(MargoGetAppPkgCmd)
	MargoGetCmd.AddCommand(MargoGetDeploymentCmd)

	// Add main commands to Margo
	MargoCmd.AddCommand(MargoDeleteCmd)
	MargoCmd.AddCommand(MargoListCmd)
	MargoCmd.AddCommand(MargoGetCmd)

	// Add Margo to root
	RootCmd.AddCommand(MargoCmd)

	fmt.Printf("┌─────────────────────────────────────────┐\n")
	fmt.Printf("│              Server Config              │\n")
	fmt.Printf("├─────────────────────────────────────────┤\n")
	fmt.Printf("│ Host:      %-28s │\n", margoServerHost)
	fmt.Printf("│ Port:      %-28d │\n", margoServerPort)
	fmt.Printf("│ Basepath:      %-28s │\n", northboundBaseURL)
	fmt.Printf("└─────────────────────────────────────────┘\n")
}

func displayDevicesTable(resp nbi.DeviceListResp) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	// Set headers
	t.AppendHeader(table.Row{
		"ID", "Signature", "Capabilities", "State", "Created",
	})

	// Add data rows
	for _, device := range resp.Items {
		cap, _ := json.Marshal(device.Spec.Capabilities)
		row := table.Row{
			truncateString(*device.Metadata.Id, 40),
			truncateString(device.Spec.Signature, 10),
			truncateString(string(cap), 10),
			string(device.State.Onboard),
			formatTime(*device.Metadata.CreationTimestamp),
		}
		t.AppendRow(row)
	}

	// Add footer with pagination
	t.AppendFooter(table.Row{
		"", "", "",
		fmt.Sprintf("Page %d/%d", 1, 1),
		fmt.Sprintf("Total: %d", 1), //resp.Metadata.TotalItems),
	})

	// Configure column settings
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, WidthMax: 48}, // ID
		{Number: 2, WidthMax: 25}, // Name
		{Number: 3, WidthMax: 15}, // Type
		{Number: 4, WidthMax: 12}, // State
		{Number: 5, WidthMax: 16}, // Created
		{Number: 6, WidthMax: 16}, // Updated
	})

	t.Render()
}
func displayAppPackagesTable(resp nbi.ApplicationPackageListResp) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	// Set headers
	t.AppendHeader(table.Row{
		"ID", "Name", "Version", "Operation", "State",
		"Source Type", "Source", "Created", "Updated",
	})

	// Add data rows
	for _, pkg := range resp.Items {
		var version string
		if pkg.Spec.SourceType == "GIT_REPO" {
			gitRepo, err := pkg.Spec.Source.AsGitRepo()
			if err == nil {
				version = gitRepo.Url
			}
		}
		// fmt.Println("-----------------------pkg------------------", pretty.Sprint(pkg))
		row := table.Row{
			truncateString(*pkg.Metadata.Id, 40),
			truncateString(pkg.Metadata.Name, 20),
			version,
			string(pkg.RecentOperation.Op),
			string(*pkg.Status.State),
			string(pkg.Spec.SourceType),
			extractSource(pkg.Spec.Source),
			formatTime(*pkg.Metadata.CreationTimestamp),
			formatTime(*pkg.Status.LastUpdateTime),
		}

		t.AppendRow(row)
	}

	// Add footer with pagination
	t.AppendFooter(table.Row{
		"", "", "", "", "", "", "",
		fmt.Sprintf("Page %d/%d", 1, 1), //resp.Metadata.Page, resp.Metadata.TotalPages),
		fmt.Sprintf("Total: %d", 1),     //resp.Metadata.TotalItems),
	})

	// Configure column settings
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, WidthMax: 48}, // ID
		{Number: 2, WidthMax: 25}, // Name
		{Number: 3, WidthMax: 10}, // Version
		{Number: 4, WidthMax: 12}, // Operation
		{Number: 5, WidthMax: 12}, // State
		{Number: 6, WidthMax: 12}, // Source Type
		{Number: 7, WidthMax: 35}, // Source
		{Number: 8, WidthMax: 16}, // Created
		{Number: 9, WidthMax: 16}, // Updated
	})

	t.Render()
}

func displayDeploymentsTable(resp nbi.ApplicationDeploymentListResp) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	// Set headers
	t.AppendHeader(table.Row{
		"ID", "Name", "Pkg", "Device", "Op" /*"OpStatus",*/, "RunningState" /*"Created",*/, "Updated",
	})

	// Add data rows
	for _, dep := range resp.Items {
		row := table.Row{
			truncateString(*dep.Metadata.Id, 48),
			truncateString(dep.Metadata.Name, 10),
			truncateString(dep.Spec.AppPackageRef.Id, 10),
			truncateString(*dep.Spec.DeviceRef.Id, 10),
			string(dep.RecentOperation.Op),
			/*string(dep.RecentOperation.Status),*/
			string(*dep.Status.State),
			// formatTime(*dep.Metadata.CreationTimestamp),
			formatTime(*dep.Status.LastUpdateTime),
		}

		t.AppendRow(row)
	}

	// Add footer with pagination
	t.AppendFooter(table.Row{
		"", "", "", "", "", "",
		fmt.Sprintf("Page %d/%d", 1, 1), //resp.Metadata.Page, resp.Metadata.TotalPages),
		fmt.Sprintf("Total: %d", 1),     //resp.Metadata.TotalItems),
	})

	// Configure column settings
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, WidthMax: 48}, // ID
		{Number: 2, WidthMax: 25}, // Name
		{Number: 3, WidthMax: 35}, // Pkg
		{Number: 4, WidthMax: 35}, // Device
		{Number: 5, WidthMax: 12}, // Op
		//{Number: 6, WidthMax: 12}, // OpStatus
		{Number: 7, WidthMax: 12}, // RunningState
		// {Number: 8, WidthMax: 16}, // Created
		{Number: 9, WidthMax: 16}, // Updated
	})

	t.Render()
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func formatTime(t time.Time) string {
	return t.Format("2006-01-02 15:04")
}

func extractSource(source nbi.ApplicationPackageSpec_Source) string {
	gitRepo, err := source.AsGitRepo()
	if err == nil {
		jsonData, _ := json.Marshal(gitRepo)
		return string(jsonData)
	}
	return "N/A"
}

func printAppPkgDetails(appPkg *nbi.ApplicationPackageManifestResp) {
	if appPkg == nil {
		fmt.Println("No application package details found.")
		return
	}

	fmt.Printf("  ID: %s\n", *appPkg.Metadata.Id)
	fmt.Printf("  Name: %s\n", appPkg.Metadata.Name)
	fmt.Printf("  API Version: %s\n", appPkg.ApiVersion)
	fmt.Printf("  Kind: %s\n", appPkg.Kind)

	fmt.Printf("  Metadata:\n")
	fmt.Printf("    Creation Timestamp: %s\n", appPkg.Metadata.CreationTimestamp)
	fmt.Printf("    Namespace: %s\n", *appPkg.Metadata.Namespace)

	fmt.Printf("  Spec:\n")
	fmt.Printf("    Source Type: %s\n", appPkg.Spec.SourceType)

	gitRepo, err := appPkg.Spec.Source.AsGitRepo()
	if err == nil {
		fmt.Printf("    Git Source:\n")
		fmt.Printf("      URL: %s\n", gitRepo.Url)
		fmt.Printf("      Branch: %s\n", *gitRepo.Branch)
		fmt.Printf("      Tag: %s\n", *gitRepo.Tag)
		fmt.Printf("      Username: %s\n", *gitRepo.Username)
		fmt.Printf("      SubPath: %s\n", *gitRepo.SubPath)
	}

	fmt.Printf("  Status:\n")
	fmt.Printf("    State: %s\n", *appPkg.Status.State)
	fmt.Printf("    Last Update Time: %s\n", appPkg.Status.LastUpdateTime)
}

func printDeploymentDetails(deployment *nbi.ApplicationDeploymentManifestResp) {
	if deployment == nil {
		fmt.Println("No application deployment details found.")
		return
	}

	fmt.Printf("  ID: %s\n", *deployment.Metadata.Id)
	fmt.Printf("  Name: %s\n", deployment.Metadata.Name)
	fmt.Printf("  API Version: %s\n", deployment.ApiVersion)
	fmt.Printf("  Kind: %s\n", deployment.Kind)

	fmt.Printf("  Metadata:\n")
	fmt.Printf("    Creation Timestamp: %s\n", deployment.Metadata.CreationTimestamp)
	fmt.Printf("    Namespace: %s\n", *deployment.Metadata.Namespace)

	fmt.Printf("  Spec:\n")
	fmt.Printf("    Package Id: %s\n", deployment.Spec.AppPackageRef.Id)
	fmt.Printf("    Device Id: %s\n", *deployment.Spec.DeviceRef.Id)

	fmt.Printf("  Status:\n")
	fmt.Printf("    State: %s\n", *deployment.Status.State)
	fmt.Printf("    Last Update Time: %s\n", deployment.Status.LastUpdateTime)
}

func convertYamlToJson(yamldata []byte) ([]byte, error) {
	return yaml.YAMLToJSON(yamldata)
}
