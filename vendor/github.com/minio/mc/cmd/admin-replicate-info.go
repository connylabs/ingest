// Copyright (c) 2015-2021 MinIO, Inc.
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
	"strings"

	"github.com/fatih/color"
	"github.com/minio/cli"
	json "github.com/minio/colorjson"
	"github.com/minio/madmin-go"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/console"
)

var adminReplicateInfoCmd = cli.Command{
	Name:         "info",
	Usage:        "get site replication information",
	Action:       mainAdminReplicationInfo,
	OnUsageError: onUsageError,
	Before:       setGlobalsFromContext,
	Flags:        globalFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} ALIAS1

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. Get Site Replication information:
     {{.Prompt}} {{.HelpName}} minio1
`,
}

type srInfo madmin.SiteReplicationInfo

func (i srInfo) JSON() string {
	bs, e := json.MarshalIndent(madmin.SiteReplicationInfo(i), "", " ")
	fatalIf(probe.NewError(e), "Unable to marshal into JSON.")
	return string(bs)
}

func (i srInfo) String() string {
	var messages []string
	info := madmin.SiteReplicationInfo(i)
	if info.Enabled {
		messages = []string{
			"SiteReplication enabled for:\n",
		}
		r := console.Colorize("THeaders", newPrettyTable(" | ",
			Field{"Deployment ID", 36},

			Field{"Name", 15},
			Field{"Endpoint", 50},
		).buildRow("Deployment ID", "Site Name", "Endpoint"))
		messages = append(messages, r)
		for _, peer := range info.Sites {
			r := console.Colorize("TDetail", newPrettyTable(" | ",
				Field{"Deployment ID", 36},

				Field{"Name", 15},
				Field{"Endpoint", 50},
			).buildRow(peer.DeploymentID, peer.Name, peer.Endpoint))
			messages = append(messages, r)
		}
	} else {
		messages = []string{"SiteReplication is not enabled"}
	}

	return console.Colorize("UserMessage", strings.Join(messages, "\n"))
}

func mainAdminReplicationInfo(ctx *cli.Context) error {
	{
		// Check argument count
		argsNr := len(ctx.Args())
		if argsNr != 1 {
			fatalIf(errInvalidArgument().Trace(ctx.Args().Tail()...),
				"Need exactly one alias argument.")
		}
	}

	console.SetColor("UserMessage", color.New(color.FgGreen))
	console.SetColor("THeaders", color.New(color.Bold, color.FgHiWhite))
	console.SetColor("TDetail", color.New(color.Bold, color.FgCyan))

	// Get the alias parameter from cli
	args := ctx.Args()
	aliasedURL := args.Get(0)

	// Create a new MinIO Admin Client
	client, err := newAdminClient(aliasedURL)
	fatalIf(err, "Unable to initialize admin connection.")

	info, e := client.SiteReplicationInfo(globalContext)
	fatalIf(probe.NewError(e).Trace(args...), "Unable to get cluster replication information")

	printMsg(srInfo(info))

	return nil
}
