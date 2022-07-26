// Copyright (c) 2015-2022 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/inconshreveable/mousetrap"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/env"
	"github.com/minio/pkg/trie"
	"github.com/minio/pkg/words"

	completeinstall "github.com/posener/complete/cmd/install"
)

// global flags for mc.
var mcFlags = []cli.Flag{
	cli.BoolFlag{
		Name:  "autocompletion",
		Usage: "install auto-completion for your shell",
	},
}

// Help template for mc
var mcHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

USAGE:
  {{.Name}} {{if .VisibleFlags}}[FLAGS] {{end}}COMMAND{{if .VisibleFlags}} [COMMAND FLAGS | -h]{{end}} [ARGUMENTS...]

COMMANDS:
  {{range .VisibleCommands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
  {{end}}{{if .VisibleFlags}}
GLOBAL FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
TIP:
  Use '{{.Name}} --autocompletion' to enable shell autocompletion

COPYRIGHT:
  Copyright (c) 2015-` + CopyrightYear + ` MinIO, Inc.

LICENSE:
  GNU AGPLv3 <https://www.gnu.org/licenses/agpl-3.0.html>
`

func init() {
	if env.IsSet(mcEnvConfigFile) {
		configFile := env.Get(mcEnvConfigFile, "")
		fatalIf(readAliasesFromFile(configFile).Trace(configFile), "Unable to parse "+configFile)
	}
	if runtime.GOOS == "windows" {
		if mousetrap.StartedByExplorer() {
			fmt.Printf("Don't double-click %s\n", os.Args[0])
			fmt.Println("You need to open cmd.exe/PowerShell and run it from the command line")
			fmt.Println("Press the Enter Key to Exit")
			fmt.Scanln()
			os.Exit(1)
		}
	}
}

// Main starts mc application
func Main(args []string) {
	if len(args) > 1 {
		switch args[1] {
		case "mc", filepath.Base(args[0]):
			mainComplete()
			return
		}
	}

	// ``MC_PROFILER`` supported options are [cpu, mem, block, goroutine].
	if p := os.Getenv("MC_PROFILER"); p != "" {
		profilers := strings.Split(p, ",")
		if e := enableProfilers(mustGetProfileDir(), profilers); e != nil {
			console.Fatal(e)
		}
	}

	probe.Init() // Set project's root source path.
	probe.SetAppInfo("Release-Tag", ReleaseTag)
	probe.SetAppInfo("Commit", ShortCommitID)

	// Fetch terminal size, if not available, automatically
	// set globalQuiet to true.
	if w, e := pb.GetTerminalWidth(); e != nil {
		globalQuiet = true
	} else {
		globalTermWidth = w
	}

	// Set the mc app name.
	appName := filepath.Base(args[0])
	if runtime.GOOS == "windows" && strings.HasSuffix(strings.ToLower(appName), ".exe") {
		// Trim ".exe" from Windows executable.
		appName = appName[:strings.LastIndex(appName, ".")]
	}

	// Monitor OS exit signals and cancel the global context in such case
	go trapSignals(os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

	// Run the app - exit on error.
	if err := registerApp(appName).Run(args); err != nil {
		os.Exit(1)
	}
}

func flagValue(f cli.Flag) reflect.Value {
	fv := reflect.ValueOf(f)
	for fv.Kind() == reflect.Ptr {
		fv = reflect.Indirect(fv)
	}
	return fv
}

func visibleFlags(fl []cli.Flag) []cli.Flag {
	visible := []cli.Flag{}
	for _, flag := range fl {
		field := flagValue(flag).FieldByName("Hidden")
		if !field.IsValid() || !field.Bool() {
			visible = append(visible, flag)
		}
	}
	return visible
}

// Function invoked when invalid flag is passed
func onUsageError(ctx *cli.Context, err error, subcommand bool) error {
	type subCommandHelp struct {
		flagName string
		usage    string
	}

	// Calculate the maximum width of the flag name field
	// for a good looking printing
	vflags := visibleFlags(ctx.Command.Flags)
	help := make([]subCommandHelp, len(vflags))
	maxWidth := 0
	for i, f := range vflags {
		s := strings.Split(f.String(), "\t")
		if len(s[0]) > maxWidth {
			maxWidth = len(s[0])
		}

		help[i] = subCommandHelp{flagName: s[0], usage: s[1]}
	}
	maxWidth += 2

	var errMsg strings.Builder

	// Do the good-looking printing now
	fmt.Fprintln(&errMsg, "Invalid command usage,", err.Error())
	fmt.Fprintln(&errMsg, "")
	fmt.Fprintln(&errMsg, "SUPPORTED FLAGS:")
	for _, h := range help {
		spaces := string(bytes.Repeat([]byte{' '}, maxWidth-len(h.flagName)))
		fmt.Fprintf(&errMsg, "   %s%s%s\n", h.flagName, spaces, h.usage)
	}
	console.Fatal(errMsg.String())
	return err
}

// Function invoked when invalid command is passed.
func commandNotFound(ctx *cli.Context, cmds []cli.Command) {
	command := ctx.Args().First()
	if command == "" {
		cli.ShowCommandHelp(ctx, command)
		return
	}
	msg := fmt.Sprintf("`%s` is not a recognized command. Get help using `--help` flag.", command)
	commandsTree := trie.NewTrie()
	for _, cmd := range cmds {
		commandsTree.Insert(cmd.Name)
	}
	closestCommands := findClosestCommands(commandsTree, command)
	if len(closestCommands) > 0 {
		msg += "\n\nDid you mean one of these?\n"
		if len(closestCommands) == 1 {
			cmd := closestCommands[0]
			msg += fmt.Sprintf("        `%s`", cmd)
		} else {
			for _, cmd := range closestCommands {
				msg += fmt.Sprintf("        `%s`\n", cmd)
			}
		}
	}
	fatalIf(errDummy().Trace(), msg)
}

// Check for sane config environment early on and gracefully report.
func checkConfig() {
	// Refresh the config once.
	loadMcConfig = loadMcConfigFactory()
	// Ensures config file is sane.
	config, err := loadMcConfig()
	// Verify if the path is accesible before validating the config
	fatalIf(err.Trace(mustGetMcConfigPath()), "Unable to access configuration file.")

	// Validate and print error messges
	ok, errMsgs := validateConfigFile(config)
	if !ok {
		var errorMsg bytes.Buffer
		for index, errMsg := range errMsgs {
			// Print atmost 10 errors
			if index > 10 {
				break
			}
			errorMsg.WriteString(errMsg + "\n")
		}
		console.Fatal(errorMsg.String())
	}
}

func migrate() {
	// Fix broken config files if any.
	fixConfig()

	// Migrate config files if any.
	migrateConfig()

	// Migrate session files if any.
	migrateSession()

	// Migrate shared urls if any.
	migrateShare()
}

// initMC - initialize 'mc'.
func initMC() {
	// Check if mc config exists.
	if !isMcConfigExists() {
		err := saveMcConfig(newMcConfig())
		fatalIf(err.Trace(), "Unable to save new mc config.")

		if !globalQuiet && !globalJSON {
			console.Infoln("Configuration written to `" + mustGetMcConfigPath() + "`. Please update your access credentials.")
		}
	}

	// Check if mc session directory exists.
	if !isSessionDirExists() {
		fatalIf(createSessionDir().Trace(), "Unable to create session config directory.")
	}

	// Check if mc share directory exists.
	if !isShareDirExists() {
		initShareConfig()
	}

	// Check if certs dir exists
	if !isCertsDirExists() {
		fatalIf(createCertsDir().Trace(), "Unable to create `CAs` directory.")
	}

	// Check if CAs dir exists
	if !isCAsDirExists() {
		fatalIf(createCAsDir().Trace(), "Unable to create `CAs` directory.")
	}

	// Load all authority certificates present in CAs dir
	loadRootCAs()
}

func getShellName() (string, bool) {
	shellName := os.Getenv("SHELL")
	if shellName != "" || runtime.GOOS == "windows" {
		return strings.ToLower(filepath.Base(shellName)), true
	}

	ppid := os.Getppid()
	cmd := exec.Command("ps", "-p", strconv.Itoa(ppid), "-o", "comm=")
	ppName, err := cmd.Output()
	if err != nil {
		fatalIf(probe.NewError(err), "Failed to enable autocompletion. Cannot determine shell type and "+
			"no SHELL environment variable found")
	}
	shellName = strings.TrimSpace(string(ppName))
	return strings.ToLower(filepath.Base(shellName)), false
}

func installAutoCompletion() {
	if runtime.GOOS == "windows" {
		console.Infoln("autocompletion feature is not available for this operating system")
		return
	}

	shellName, ok := getShellName()
	if !ok {
		console.Infoln("No 'SHELL' env var. Your shell is auto determined as '" + shellName + "'.")
	} else {
		console.Infoln("Your shell is set to '" + shellName + "', by env var 'SHELL'.")
	}

	supportedShellsSet := set.CreateStringSet("bash", "zsh", "fish")
	if !supportedShellsSet.Contains(shellName) {
		fatalIf(probe.NewError(errors.New("")),
			"'"+shellName+"' is not a supported shell. "+
				"Supported shells are: bash, zsh, fish")
	}

	err := completeinstall.Install(filepath.Base(os.Args[0]))
	var printMsg string
	if err != nil && strings.Contains(err.Error(), "* already installed") {
		errStr := err.Error()[strings.Index(err.Error(), "\n")+1:]
		re := regexp.MustCompile(`[::space::]*\*.*` + shellName + `.*`)
		relatedMsg := re.FindStringSubmatch(errStr)
		if len(relatedMsg) > 0 {
			printMsg = "\n" + relatedMsg[0]
		} else {
			printMsg = ""
		}
	}
	if printMsg != "" {
		if completeinstall.IsInstalled(filepath.Base(os.Args[0])) || completeinstall.IsInstalled("mc") {
			console.Infoln("autocompletion is enabled.", printMsg)
		} else {
			fatalIf(probe.NewError(err), "Unable to install auto-completion.")
		}
	} else {
		console.Infoln("enabled autocompletion in your '" + shellName + "' rc file. Please restart your shell.")
	}
}

func registerBefore(ctx *cli.Context) error {
	if ctx.IsSet("config-dir") {
		// Set the config directory.
		setMcConfigDir(ctx.String("config-dir"))
	} else if ctx.GlobalIsSet("config-dir") {
		// Set the config directory.
		setMcConfigDir(ctx.GlobalString("config-dir"))
	}

	// Set global flags.
	setGlobalsFromContext(ctx)

	// Migrate any old version of config / state files to newer format.
	migrate()

	// Initialize default config files.
	initMC()

	// Check if config can be read.
	checkConfig()

	return nil
}

// findClosestCommands to match a given string with commands trie tree.
func findClosestCommands(commandsTree *trie.Trie, command string) []string {
	closestCommands := commandsTree.PrefixMatch(command)
	sort.Strings(closestCommands)
	// Suggest other close commands - allow missed, wrongly added and even transposed characters
	for _, value := range commandsTree.Walk(commandsTree.Root()) {
		if sort.SearchStrings(closestCommands, value) < len(closestCommands) {
			continue
		}
		// 2 is arbitrary and represents the max allowed number of typed errors
		if words.DamerauLevenshteinDistance(command, value) < 2 {
			closestCommands = append(closestCommands, value)
		}
	}
	return closestCommands
}

// Check for updates and print a notification message
func checkUpdate(ctx *cli.Context) {
	// Do not print update messages, if quiet flag is set.
	if ctx.Bool("quiet") || ctx.GlobalBool("quiet") {
		// Its OK to ignore any errors during doUpdate() here.
		if updateMsg, _, currentReleaseTime, latestReleaseTime, err := getUpdateInfo(2 * time.Second); err == nil {
			printMsg(updateMessage{
				Status:  "success",
				Message: updateMsg,
			})
		} else {
			printMsg(updateMessage{
				Status:  "success",
				Message: prepareUpdateMessage("Run `mc update`", latestReleaseTime.Sub(currentReleaseTime)),
			})
		}
	}
}

var appCmds = []cli.Command{
	aliasCmd,
	lsCmd,
	mbCmd,
	rbCmd,
	cpCmd,
	mvCmd,
	rmCmd,
	mirrorCmd,
	catCmd,
	headCmd,
	pipeCmd,
	findCmd,
	sqlCmd,
	statCmd,
	treeCmd,
	duCmd,
	retentionCmd,
	legalHoldCmd,
	supportCmd,
	licenseCmd,
	shareCmd,
	versionCmd,
	ilmCmd,
	encryptCmd,
	eventCmd,
	watchCmd,
	undoCmd,
	anonymousCmd,
	policyCmd,
	tagCmd,
	diffCmd,
	replicateCmd,
	adminCmd,
	configCmd,
	updateCmd,
	readyCmd,
	pingCmd,
}

func printMCVersion(c *cli.Context) {
	fmt.Fprintf(c.App.Writer, "%s version %s (commit-id=%s)\n", c.App.Name, c.App.Version, CommitID)
	fmt.Fprintf(c.App.Writer, "Runtime: %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(c.App.Writer, "Copyright (c) 2015-%s MinIO, Inc.\n", CopyrightYear)
	fmt.Fprintf(c.App.Writer, "License GNU AGPLv3 <https://www.gnu.org/licenses/agpl-3.0.html>\n")
}

func registerApp(name string) *cli.App {
	cli.HelpFlag = cli.BoolFlag{
		Name:  "help, h",
		Usage: "show help",
	}

	// Override default cli version printer
	cli.VersionPrinter = printMCVersion

	app := cli.NewApp()
	app.Name = name
	app.Action = func(ctx *cli.Context) error {
		if strings.HasPrefix(ReleaseTag, "RELEASE.") {
			// Check for new updates from dl.min.io.
			checkUpdate(ctx)
		}

		if ctx.Bool("autocompletion") || ctx.GlobalBool("autocompletion") {
			// Install shell completions
			installAutoCompletion()
			return nil
		}

		if ctx.Args().First() != "" {
			commandNotFound(ctx, app.Commands)
		} else {
			cli.ShowAppHelp(ctx)
		}

		return exitStatus(globalErrorExitStatus)
	}

	app.Before = registerBefore
	app.HideHelpCommand = true
	app.Usage = "MinIO Client for object storage and filesystems."
	app.Commands = appCmds
	app.Author = "MinIO, Inc."
	app.Version = ReleaseTag
	app.Flags = append(mcFlags, globalFlags...)
	app.CustomAppHelpTemplate = mcHelpTemplate
	app.EnableBashCompletion = true
	app.OnUsageError = onUsageError

	return app
}

// mustGetProfilePath must get location that the profile will be written to.
func mustGetProfileDir() string {
	return filepath.Join(mustGetMcConfigDir(), globalProfileDir)
}
