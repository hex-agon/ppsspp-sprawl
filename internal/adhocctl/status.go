package adhocctl

import "sprawl/internal/titledb"

type GroupStatus struct {
	ID        string   `json:"id"`
	Players   []string `json:"players"`
	CreatedAt int64    `json:"created_at"`
}

type TitleStatus struct {
	TitleID     string        `json:"title_id"`
	Name        string        `json:"name"`
	Groups      []GroupStatus `json:"groups"`
	PlayerCount int           `json:"player_count,omitzero"`
}

type CtlClientInfo struct {
	MAC      string `json:"mac"`
	Name     string `json:"name"`
	IP       string `json:"ip"`
	TitleID  string `json:"title_id"`
	GroupID  string `json:"group_id"`
	IsLeader bool   `json:"is_leader"`
}

func (srv *ctlServer) GetActiveTitles() []TitleStatus {
	srv.RLock()
	defer srv.RUnlock()

	result := make([]TitleStatus, 0, len(srv.groups))

	for tid, groups := range srv.groups {
		if len(groups) == 0 {
			continue
		}

		titleStatus := TitleStatus{
			TitleID: tid.String(),
			Groups:  make([]GroupStatus, 0, len(groups)),
		}

		if name, ok := titledb.GetTitle(tid); ok {
			titleStatus.Name = name
		} else {
			titleStatus.Name = tid.String()
		}

		for gid, g := range groups {
			g.RLock()
			groupStatus := GroupStatus{
				ID:        gid.String(),
				Players:   make([]string, 0, len(g.clients)),
				CreatedAt: g.createdAt,
			}
			for c := range g.clients {
				groupStatus.Players = append(groupStatus.Players, c.name.String())
			}
			g.RUnlock()
			titleStatus.PlayerCount += len(groupStatus.Players)
			titleStatus.Groups = append(titleStatus.Groups, groupStatus)
		}
		result = append(result, titleStatus)
	}

	return result
}

func (srv *ctlServer) GetClients() []CtlClientInfo {
	srv.RLock()
	defer srv.RUnlock()

	result := make([]CtlClientInfo, 0, len(srv.clients))
	for _, cli := range srv.clients {
		info := CtlClientInfo{
			MAC:     cli.macAddr.String(),
			Name:    cli.name.String(),
			IP:      cli.conn.RemoteAddr().String(),
			TitleID: cli.titleId.String(),
		}
		if cli.group != nil {
			info.GroupID = cli.group.id.String()
			info.IsLeader = cli.macAddr == cli.group.founderMacAddr
		}
		result = append(result, info)
	}
	return result
}
