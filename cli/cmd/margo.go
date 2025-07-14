package cmd

// the following script should accept things in the following format:
// margo upload app-pkg --from-file --file=""
// margo upload app-pkg --from-git --git.url="" --git.username="" --git.token=""
// margo delete app-pkg <app-pkg-id>
// margo list app-pkg
// margo get app-pkg <app-pkg-id>

import (
	"fmt"
	"os"
	"time"

	"github.com/eclipse-symphony/symphony/cli/utils"
	"github.com/jedib0t/go-pretty/v6/table"
	margoCli "github.com/margo/dev-repo/non-standard/cli/wfm"
	"github.com/margo/dev-repo/non-standard/generatedCode/models"
	"github.com/spf13/cobra"
)

var (
	// margo compliant server details
	margoServerHost   string
	margoServerPort   uint16
	northboundBaseURL = "v1alpha2/margo/northbound/v1"
)

var (
	// Pkg related variables
	pkgName        string
	pkgFromFile    bool
	pkgFromGit     bool
	pkgFilePath    string
	pkgGitURL      string
	pkgGitUsername string
	pkgGitToken    string
	pkgGitBranch   string
	pkgGitTag      string
	pkgGitAppPath  string

	// pkgIdToDelete string
)

var MargoCmd = &cobra.Command{
	Use:   "wfm",
	Short: "Margo commands for managing margo applications",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("\n%sMargo CLI - Use subcommands: upload, delete, list, get%s\n\n", utils.ColorBlue(), utils.ColorReset())
		cmd.Help()
	},
}

// Upload command
var MargoOnboardCmd = &cobra.Command{
	Use:   "upload",
	Short: "Upload application Pkgs",
}

var MargoOnboardAppPkgCmd = &cobra.Command{
	Use:   "app-pkg",
	Short: "Upload a margo application Pkg",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("┌─────────────────────────────────────────┐\n")
		fmt.Printf("│ From File: %-28t │\n", pkgFromFile)
		fmt.Printf("│ From Git:  %-28t │\n", pkgFromGit)
		fmt.Printf("│ File Path: %-28s │\n", pkgFilePath)
		fmt.Printf("│ Git URL:   %-28s │\n", pkgGitURL)
		fmt.Printf("└─────────────────────────────────────────┘\n")

		if err := validateUploadFlags(); err != nil {
			fmt.Printf("\n%s%s%s\n\n", utils.ColorRed(), err.Error(), utils.ColorReset())
			return
		}

		if err := onboardAppPkg(); err != nil {
			fmt.Printf("\n%sUpload failed: %s%s\n\n", utils.ColorRed(), err.Error(), utils.ColorReset())
			return
		}

		fmt.Printf("\n%sApplication Pkg uploaded successfully%s\n\n", utils.ColorGreen(), utils.ColorReset())
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

// List command
var MargoListCmd = &cobra.Command{
	Use:   "list",
	Short: "List application Pkgs",
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

// Validation functions
func validateUploadFlags() error {
	if !pkgFromFile && !pkgFromGit {
		return fmt.Errorf("either --from-file or --from-git must be specified")
	}
	if pkgFromFile && pkgFromGit {
		return fmt.Errorf("only one of --from-file or --from-git can be specified")
	}
	if pkgFromFile && pkgFilePath == "" {
		return fmt.Errorf("--file must be provided when using --from-file")
	}
	if pkgFromGit && pkgGitURL == "" {
		return fmt.Errorf("--git.url must be provided when using --from-git")
	}
	if pkgGitBranch == "" && pkgGitTag == "" {
		return fmt.Errorf("either --git.branch or --git.tag must be specified")
	}
	return nil
}

// Implementation functions
func onboardAppPkg() error {
	if pkgFromFile {
		return fmt.Errorf("upload from file is not supported")
	}

	var appPkgReq *margoCli.AppPkgOnboardingReq

	northboundCli := margoCli.NewNorthboundCli(margoServerHost, margoServerPort, &northboundBaseURL)

	if pkgFromGit {
		gitRepo := &models.GitRepo{
			Url: pkgGitURL,
		}

		if pkgGitBranch != "" {
			gitRepo.Branch = &pkgGitBranch
		}

		if pkgGitUsername != "" && pkgGitToken != "" {
			gitRepo.AccessToken = &pkgGitToken
			gitRepo.Username = &pkgGitUsername
		}

		if pkgGitAppPath != "" {
			gitRepo.SubPath = &pkgGitAppPath
		}

		source := &models.AppPkgOnboardingReq_Source{}
		if err := source.FromGitRepo(*gitRepo); err != nil {
			return fmt.Errorf("failed to parse the git repo details into cli spec model: %w", err)
		}

		// Handle Git upload
		appPkgReq = &margoCli.AppPkgOnboardingReq{
			Name:       pkgName,
			Source:     *source,
			SourceType: models.AppPkgOnboardingReqSourceTypeGITREPO,
		}
	}

	if appPkgReq == nil {
		return fmt.Errorf("no application Pkg specified")
	}

	resp, err := northboundCli.OnboardAppPkg(*appPkgReq)
	if err != nil {
		return fmt.Errorf("failed to upload application Pkg: %w", err)
	}

	if resp == nil {
		fmt.Println("response is nil")
		return nil
	}

	fmt.Println("pkgId", resp.Id, "pkgName", resp.Name, "pkgVersion", resp.Version)
	return nil
}

func deleteAppPkg(appPkgID string) error {
	northboundCli := margoCli.NewNorthboundCli(margoServerHost, margoServerPort, &northboundBaseURL)

	err := northboundCli.DeleteAppPkg(appPkgID)
	if err != nil {
		return fmt.Errorf("failed to delete application Pkg: %w", err)
	}

	fmt.Println("app Pkg deletion request has been accepted!")
	return nil
}

func listAppPkgs() error {
	northboundCli := margoCli.NewNorthboundCli(margoServerHost, margoServerPort, &northboundBaseURL)

	appPkgs, err := northboundCli.ListAppPkgs(margoCli.ListAppPkgsParams{})
	if err != nil {
		return fmt.Errorf("failed to list application Pkgs: %w", err)
	}

	displayAppPackagesTable(*appPkgs)
	return nil
}

func getAppPkg(appPkgID string) error {
	northboundCli := margoCli.NewNorthboundCli(margoServerHost, margoServerPort, &northboundBaseURL)

	appPkg, err := northboundCli.GetAppPkg(appPkgID)
	if err != nil {
		return fmt.Errorf("failed to get application Pkg: %w", err)
	}

	// Display the details
	fmt.Printf("\n%sApplication Pkg Details:%s\n", utils.ColorBlue(), utils.ColorReset())
	fmt.Println("Pkg", appPkg)
	fmt.Println()
	return nil
}

func init() {
	// Upload command flags
	// Global flags that apply to all Margo commands
	MargoCmd.PersistentFlags().StringVar(&margoServerHost, "host", "localhost", "Margo compliant WFM API server host")
	MargoCmd.PersistentFlags().Uint16Var(&margoServerPort, "port", 8082, "Margo compliant WFM API server port")
	MargoOnboardAppPkgCmd.Flags().StringVar(&pkgName, "name", "", "Name of the uploaded pkg name")
	MargoOnboardAppPkgCmd.Flags().BoolVar(&pkgFromFile, "from-file", false, "Upload from file")
	MargoOnboardAppPkgCmd.Flags().BoolVar(&pkgFromGit, "from-git", false, "Upload from Git repository")
	MargoOnboardAppPkgCmd.Flags().StringVar(&pkgFilePath, "file", "", "Application Pkg file path")
	MargoOnboardAppPkgCmd.Flags().StringVar(&pkgGitURL, "git.url", "", "Git repository URL")
	MargoOnboardAppPkgCmd.Flags().StringVar(&pkgGitBranch, "git.branch", "main", "Git branch (default: main)")
	MargoOnboardAppPkgCmd.Flags().StringVar(&pkgGitTag, "git.tag", "latest", "Git tag (default: latest)")
	MargoOnboardAppPkgCmd.Flags().StringVar(&pkgGitUsername, "git.username", "", "Git username for authentication")
	MargoOnboardAppPkgCmd.Flags().StringVar(&pkgGitToken, "git.token", "", "Git token for authentication")
	MargoOnboardAppPkgCmd.Flags().StringVar(&pkgGitAppPath, "git.appPath", ".", "Path to the app description file in the git repo")

	// MargoDeleteAppPkgCmd.Flags().StringVar(&pkgIdToDelete, "", ".", "Path to the app description file in the git repo")

	// Build command hierarchy
	MargoOnboardCmd.AddCommand(MargoOnboardAppPkgCmd)
	MargoDeleteCmd.AddCommand(MargoDeleteAppPkgCmd)
	MargoListCmd.AddCommand(MargoListAppPkgCmd)
	MargoGetCmd.AddCommand(MargoGetAppPkgCmd)

	// Add main commands to Margo
	MargoCmd.AddCommand(MargoOnboardCmd)
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

func displayAppPackagesTable(resp models.ListAppPkgsResp) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	// Set headers
	t.AppendHeader(table.Row{
		"ID", "Name", "Version", "Operation", "State",
		"Source Type", "Source", "Created", "Updated",
	})

	// Add data rows
	for _, pkg := range resp.AppPkgs {
		row := table.Row{
			truncateString(pkg.Id, 48),
			truncateString(pkg.Name, 20),
			pkg.Version,
			string(pkg.Operation),
			string(pkg.OperationState),
			string(pkg.SourceType),
			extractSource(pkg.Source),
			formatTime(pkg.CreatedAt),
			formatTime(pkg.UpdatedAt),
		}

		t.AppendRow(row)
	}

	// Add footer with pagination
	t.AppendFooter(table.Row{
		"", "", "", "", "", "", "",
		fmt.Sprintf("Page %d/%d", resp.Pagination.Page, resp.Pagination.TotalPages),
		fmt.Sprintf("Total: %d", resp.Pagination.TotalItems),
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

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func formatTime(t time.Time) string {
	return t.Format("2006-01-02 15:04")
}

func extractSource(source models.AppPkgSummary_Source) string {
	// Implement based on your actual source structure
	jsonBytes, err := source.MarshalJSON()

	if err != nil {
		return "N/A"
	}
	return truncateString(string(jsonBytes), 34)
}
