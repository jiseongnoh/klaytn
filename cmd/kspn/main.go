// Modifications Copyright 2019 The klaytn Authors
// Copyright 2016 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.
//
// This file is derived from cmd/geth/main.go (2018/06/04).
// Modified and improved for the klaytn development.

package main

import (
	"fmt"
	"os"
	"runtime"
	"sort"

	"github.com/klaytn/klaytn/api/debug"
	"github.com/klaytn/klaytn/cmd/utils"
	"github.com/klaytn/klaytn/cmd/utils/nodecmd"
	"github.com/klaytn/klaytn/console"
	"github.com/klaytn/klaytn/log"
	metricutils "github.com/klaytn/klaytn/metrics/utils"
	"github.com/klaytn/klaytn/node"
	"gopkg.in/urfave/cli.v1"
)

var (
	logger = log.NewModuleLogger(log.CMDKSPN)

	// The app that holds all commands and flags.
	app = utils.NewApp(nodecmd.GetGitCommit(), "The command line interface for Klaytn Proxy Node")

	// flags that configure the node
	nodeFlags = append(nodecmd.CommonNodeFlags, nodecmd.KSPNFlags...)

	rpcFlags = nodecmd.CommonRPCFlags
)

func init() {
	utils.InitHelper()
	// Initialize the CLI app and start kspn
	app.Action = nodecmd.RunKlaytnNode
	app.HideVersion = true // we have a command to print the version
	app.Copyright = "Copyright 2018-2019 The klaytn Authors"
	app.Commands = []cli.Command{
		// See utils/nodecmd/chaincmd.go:
		nodecmd.InitCommand,

		// See utils/nodecmd/accountcmd.go
		nodecmd.AccountCommand,

		// See utils/nodecmd/consolecmd.go:
		nodecmd.GetConsoleCommand(nodeFlags, rpcFlags),
		nodecmd.AttachCommand,

		// See utils/nodecmd/versioncmd.go:
		nodecmd.VersionCommand,

		// See utils/nodecmd/dumpconfigcmd.go:
		nodecmd.GetDumpConfigCommand(nodeFlags, rpcFlags),
	}
	sort.Sort(cli.CommandsByName(app.Commands))

	app.Flags = append(app.Flags, nodeFlags...)
	app.Flags = append(app.Flags, rpcFlags...)
	app.Flags = append(app.Flags, nodecmd.ConsoleFlags...)
	app.Flags = append(app.Flags, debug.Flags...)

	cli.AppHelpTemplate = utils.GlobalAppHelpTemplate
	cli.HelpPrinter = utils.NewHelpPrinter(utils.CategorizeFlags(app.Flags))

	app.CommandNotFound = nodecmd.CommandNotExist

	app.Before = func(ctx *cli.Context) error {
		runtime.GOMAXPROCS(runtime.NumCPU())
		logDir := (&node.Config{DataDir: utils.MakeDataDir(ctx)}).ResolvePath("logs")
		debug.CreateLogDir(logDir)
		if err := debug.Setup(ctx); err != nil {
			return err
		}
		metricutils.StartMetricCollectionAndExport(ctx)
		utils.SetupNetwork(ctx)
		return nil
	}

	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		console.Stdin.Close() // Resets terminal mode.
		return nil
	}
}

func main() {
	// Set NodeTypeFlag to spn
	utils.NodeTypeFlag.Value = "spn"

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
